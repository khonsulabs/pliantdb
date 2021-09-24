use std::{
    fmt::Debug,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use super::{
    btree_root::{ChangeResult, EntryChanges},
    interior::Interior,
    key_entry::KeyEntry,
    modify::{Modification, Operation},
    serialization::BinarySerialization,
    KeyRange, PagedWriter,
};
use crate::{tree::KeyEvaluation, Buffer, ChunkCache, Error, ManagedFile, Vault};

/// A B-Tree entry that stores a list of key-`I` pairs.
#[derive(Clone, Debug)]
pub struct BTreeEntry<I, R> {
    pub dirty: bool,
    node: BTreeNode<I, R>,
}

/// A B-Tree entry that stores a list of key-`I` pairs.
#[derive(Clone, Debug)]
enum BTreeNode<I, R> {
    Uninitialized,
    /// An inline value. Overall, the B-Tree entry is a key-value pair.
    Leaf(Vec<KeyEntry<I>>),
    /// An interior node that contains pointers to other nodes.
    Interior(Vec<Interior<I, R>>),
}

impl<I, R> From<BTreeNode<I, R>> for BTreeEntry<I, R> {
    fn from(node: BTreeNode<I, R>) -> Self {
        Self { node, dirty: true }
    }
}

impl<I, R> Default for BTreeEntry<I, R> {
    fn default() -> Self {
        Self::from(BTreeNode::Leaf(Vec::new()))
    }
}

pub trait Reducer<I> {
    fn reduce(indexes: &[&I]) -> Self;

    fn rereduce(reductions: &[&Self]) -> Self;
}

impl<I> Reducer<I> for () {
    fn reduce(_indexes: &[&I]) -> Self {}

    fn rereduce(_reductions: &[&Self]) -> Self {}
}

pub struct ModificationContext<T, F, I, Indexer, Loader>
where
    Indexer: Fn(
        &Buffer<'_>,
        Option<&T>,
        Option<&I>,
        &mut EntryChanges,
        &mut PagedWriter<'_, F>,
    ) -> Result<KeyOperation<I>, Error>,
    Loader: Fn(&I, &mut PagedWriter<'_, F>) -> Result<Option<T>, Error>,
{
    pub current_order: usize,
    pub indexer: Indexer,
    pub loader: Loader,
    pub _phantom: PhantomData<(T, F, I)>,
}

impl<I, R> BTreeEntry<I, R>
where
    I: Clone + BinarySerialization + Debug + 'static,
    R: Clone + Reducer<I> + BinarySerialization + Debug + 'static,
{
    pub fn modify<F, T, Indexer, Loader>(
        &mut self,
        modification: &mut Modification<'_, T>,
        context: &ModificationContext<T, F, I, Indexer, Loader>,
        max_key: Option<&Buffer<'_>>,
        changes: &mut EntryChanges,
        writer: &mut PagedWriter<'_, F>,
    ) -> Result<ChangeResult<I, R>, Error>
    where
        F: ManagedFile,
        Indexer: Fn(
            &Buffer<'_>,
            Option<&T>,
            Option<&I>,
            &mut EntryChanges,
            &mut PagedWriter<'_, F>,
        ) -> Result<KeyOperation<I>, Error>,
        Loader: Fn(&I, &mut PagedWriter<'_, F>) -> Result<Option<T>, Error>,
    {
        match &mut self.node {
            BTreeNode::Leaf(children) => {
                if Self::modify_leaf(children, modification, context, max_key, changes, writer)? {
                    self.dirty = true;

                    if children.len() >= context.current_order {
                        // We need to split this leaf into two leafs, moving a new interior node using the middle element.
                        let midpoint = children.len() / 2;
                        let (_, upper_half) = children.split_at(midpoint);
                        let upper_half = BTreeNode::Leaf(upper_half.to_vec());
                        children.truncate(midpoint);

                        Ok(ChangeResult::Split(Self::from(upper_half)))
                    } else {
                        Ok(ChangeResult::Changed)
                    }
                } else {
                    Ok(ChangeResult::Unchanged)
                }
            }
            BTreeNode::Interior(children) => {
                if Self::modify_interior(children, modification, context, max_key, changes, writer)?
                {
                    self.dirty = true;

                    if children.len() >= context.current_order {
                        let midpoint = children.len() / 2;
                        let (_, upper_half) = children.split_at(midpoint);

                        // TODO this re-clones the upper-half children, but splitting a vec
                        // without causing multiple copies of data seems
                        // impossible without unsafe.
                        let upper_half = upper_half.to_vec();
                        debug_assert_eq!(midpoint + upper_half.len(), children.len());
                        children.truncate(midpoint);

                        Ok(ChangeResult::Split(Self::from(BTreeNode::Interior(
                            upper_half,
                        ))))
                    } else {
                        Ok(ChangeResult::Changed)
                    }
                } else {
                    Ok(ChangeResult::Unchanged)
                }
            }
            BTreeNode::Uninitialized => unreachable!(),
        }
    }

    #[allow(clippy::too_many_lines)] // TODO refactor, too many lines
    fn modify_leaf<F, T, Indexer, Loader>(
        children: &mut Vec<KeyEntry<I>>,
        modification: &mut Modification<'_, T>,
        context: &ModificationContext<T, F, I, Indexer, Loader>,
        max_key: Option<&Buffer<'_>>,
        changes: &mut EntryChanges,
        writer: &mut PagedWriter<'_, F>,
    ) -> Result<bool, Error>
    where
        F: ManagedFile,
        Indexer: Fn(
            &Buffer<'_>,
            Option<&T>,
            Option<&I>,
            &mut EntryChanges,
            &mut PagedWriter<'_, F>,
        ) -> Result<KeyOperation<I>, Error>,
        Loader: Fn(&I, &mut PagedWriter<'_, F>) -> Result<Option<T>, Error>,
    {
        let mut last_index = 0;
        let mut any_changes = false;
        while !modification.keys.is_empty() && children.len() < context.current_order {
            let key = modification.keys.last().unwrap();
            let search_result = children[last_index..].binary_search_by(|child| child.key.cmp(key));
            match search_result {
                Ok(matching_index) => {
                    let key = modification.keys.pop().unwrap();
                    last_index += matching_index;
                    let index = match &mut modification.operation {
                        Operation::Set(value) => (context.indexer)(
                            &key,
                            Some(value),
                            Some(&children[last_index].index),
                            changes,
                            writer,
                        )?,
                        Operation::SetEach(values) => (context.indexer)(
                            &key,
                            Some(&values.pop().ok_or_else(|| {
                                Error::message("need the same number of keys as values")
                            })?),
                            Some(&children[last_index].index),
                            changes,
                            writer,
                        )?,
                        Operation::Remove => (context.indexer)(
                            &key,
                            None,
                            Some(&children[last_index].index),
                            changes,
                            writer,
                        )?,
                        Operation::CompareSwap(callback) => {
                            let current_index = &children[last_index].index;
                            let existing_value = (context.loader)(current_index, writer)?;
                            match callback(&key, existing_value) {
                                KeyOperation::Skip => KeyOperation::Skip,
                                KeyOperation::Set(new_value) => (context.indexer)(
                                    &key,
                                    Some(&new_value),
                                    Some(current_index),
                                    changes,
                                    writer,
                                )?,
                                KeyOperation::Remove => (context.indexer)(
                                    &key,
                                    None,
                                    Some(current_index),
                                    changes,
                                    writer,
                                )?,
                            }
                        }
                    };

                    match index {
                        KeyOperation::Skip => {}
                        KeyOperation::Set(index) => {
                            children[last_index] = KeyEntry {
                                key: key.to_owned(),
                                index,
                            };
                            any_changes = true;
                        }
                        KeyOperation::Remove => {
                            children.remove(last_index);
                            any_changes = true;
                        }
                    }
                }
                Err(insert_at) => {
                    last_index += insert_at;
                    if max_key.map(|max_key| key > max_key).unwrap_or_default() {
                        break;
                    }
                    let key = modification.keys.pop().unwrap();
                    let operation = match &mut modification.operation {
                        Operation::Set(new_value) => {
                            (context.indexer)(&key, Some(new_value), None, changes, writer)?
                        }
                        Operation::SetEach(new_values) => (context.indexer)(
                            &key,
                            Some(&new_values.pop().ok_or_else(|| {
                                Error::message("need the same number of keys as values")
                            })?),
                            None,
                            changes,
                            writer,
                        )?,
                        Operation::Remove => {
                            // The key doesn't exist, so a remove is a no-op.
                            KeyOperation::Remove
                        }
                        Operation::CompareSwap(callback) => match callback(&key, None) {
                            KeyOperation::Skip => KeyOperation::Skip,
                            KeyOperation::Set(new_value) => {
                                (context.indexer)(&key, Some(&new_value), None, changes, writer)?
                            }
                            KeyOperation::Remove => {
                                (context.indexer)(&key, None, None, changes, writer)?
                            }
                        },
                    };
                    match operation {
                        KeyOperation::Set(index) => {
                            // New node.
                            if children.capacity() < children.len() + 1 {
                                children.reserve(context.current_order - children.len());
                            }
                            children.insert(
                                last_index,
                                KeyEntry {
                                    key: key.to_owned(),
                                    index,
                                },
                            );
                            any_changes = true;
                        }
                        KeyOperation::Skip | KeyOperation::Remove => {}
                    }
                }
            }
            debug_assert!(children.windows(2).all(|w| w[0].key < w[1].key),);
        }
        Ok(any_changes)
    }

    pub fn modify_interior<F, T, Indexer, Loader>(
        children: &mut Vec<Interior<I, R>>,
        modification: &mut Modification<'_, T>,
        context: &ModificationContext<T, F, I, Indexer, Loader>,
        max_key: Option<&Buffer<'_>>,
        changes: &mut EntryChanges,
        writer: &mut PagedWriter<'_, F>,
    ) -> Result<bool, Error>
    where
        F: ManagedFile,
        Indexer: Fn(
            &Buffer<'_>,
            Option<&T>,
            Option<&I>,
            &mut EntryChanges,
            &mut PagedWriter<'_, F>,
        ) -> Result<KeyOperation<I>, Error>,
        Loader: Fn(&I, &mut PagedWriter<'_, F>) -> Result<Option<T>, Error>,
    {
        let mut last_index = 0;
        let mut any_changes = false;
        while let Some(key) = modification.keys.last().cloned() {
            if children.len() >= context.current_order
                || max_key.map(|max_key| &key > max_key).unwrap_or_default()
            {
                break;
            }
            let containing_node_index = children[last_index..]
                .binary_search_by(|child| child.key.cmp(&key))
                .map_or_else(
                    |not_found| {
                        if not_found + last_index == children.len() {
                            // If we can't find a key less than what would fit
                            // within our children, this key will become the new key
                            // of the last child.
                            not_found - 1
                        } else {
                            not_found
                        }
                    },
                    |found| found,
                );

            last_index += containing_node_index;
            let child = &mut children[last_index];
            child.position.load(writer, context.current_order)?;
            let child_entry = child.position.get_mut().unwrap();
            match child_entry
                // TODO evaluate whether Some(key) is right here -- shouldn't it be the max of key/child.key?
                .modify(modification, context, Some(&key), changes, writer)?
            {
                ChangeResult::Unchanged => {}
                ChangeResult::Changed => {
                    child.key = child_entry.max_key().clone();
                    child.stats = child_entry.stats();
                    any_changes |= true;
                }
                ChangeResult::Split(upper) => {
                    child.key = child_entry.max_key().clone();
                    child.stats = child_entry.stats();

                    if children.capacity() < children.len() + 1 {
                        children.reserve(context.current_order - children.len());
                    }
                    children.insert(last_index + 1, Interior::from(upper));
                    any_changes |= true;
                }
            };
            debug_assert!(children.windows(2).all(|w| w[0].key < w[1].key));
        }
        Ok(any_changes)
    }

    pub fn split_root(&mut self, upper: Self) {
        let mut lower = Self::from(BTreeNode::Uninitialized);
        std::mem::swap(self, &mut lower);
        self.node = BTreeNode::Interior(vec![Interior::from(lower), Interior::from(upper)]);
    }

    pub fn stats(&self) -> R {
        match &self.node {
            BTreeNode::Leaf(children) => {
                R::reduce(&children.iter().map(|c| &c.index).collect::<Vec<_>>())
            }
            BTreeNode::Interior(children) => {
                R::rereduce(&children.iter().map(|c| &c.stats).collect::<Vec<_>>())
            }
            BTreeNode::Uninitialized => unreachable!(),
        }
    }

    pub fn max_key(&self) -> &Buffer<'static> {
        match &self.node {
            BTreeNode::Leaf(children) => &children.last().unwrap().key,
            BTreeNode::Interior(children) => &children.last().unwrap().key,
            BTreeNode::Uninitialized => unreachable!(),
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, key_evaluator, key_reader, file, vault, cache))
    )]
    pub fn scan<'k, F: ManagedFile, KeyRangeBounds, KeyEvaluator, KeyReader>(
        &self,
        range: &KeyRangeBounds,
        forwards: bool,
        key_evaluator: &mut KeyEvaluator,
        key_reader: &mut KeyReader,
        file: &mut F,
        vault: Option<&dyn Vault>,
        cache: Option<&ChunkCache>,
    ) -> Result<bool, Error>
    where
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        KeyReader: FnMut(Buffer<'static>, &I) -> Result<(), Error>,
        KeyRangeBounds: RangeBounds<Buffer<'k>> + Debug,
    {
        match &self.node {
            BTreeNode::Leaf(children) => {
                for child in DirectionalSliceIterator::new(forwards, children) {
                    if range.contains(&child.key) {
                        match key_evaluator(&child.key) {
                            KeyEvaluation::ReadData => {
                                key_reader(child.key.clone(), &child.index)?;
                            }
                            KeyEvaluation::Skip => {}
                            KeyEvaluation::Stop => return Ok(false),
                        };
                    }
                }
            }
            BTreeNode::Interior(children) => {
                for (index, child) in DirectionalSliceIterator::new(forwards, children).enumerate()
                {
                    // The keys in this child range from the previous child's key (exclusive) to the entry's key (inclusive).
                    let start_bound = range.start_bound();
                    let end_bound = range.end_bound();
                    if forwards {
                        if index > 0 {
                            let previous_entry = &children[index - 1];

                            // One the previous entry's key is less than the end
                            // bound, we can break out of the loop.
                            match end_bound {
                                Bound::Included(key) => {
                                    if previous_entry.key > *key {
                                        break;
                                    }
                                }
                                Bound::Excluded(key) => {
                                    if &previous_entry.key >= key {
                                        break;
                                    }
                                }
                                Bound::Unbounded => {}
                            }
                        }
                    } else {
                        // TODO need to write the logic for breaking out when iterating backwards.
                    }

                    // Keys in this child could match as long as the start bound
                    // is less than the key for this child.
                    match start_bound {
                        Bound::Included(key) => {
                            if child.key < *key {
                                continue;
                            }
                        }
                        Bound::Excluded(key) => {
                            if &child.key <= key {
                                continue;
                            }
                        }
                        Bound::Unbounded => {}
                    }

                    let keep_scanning = child.position.map_loaded_entry(
                        file,
                        vault,
                        cache,
                        children.len(),
                        |entry, file| {
                            entry.scan(
                                range,
                                forwards,
                                key_evaluator,
                                key_reader,
                                file,
                                vault,
                                cache,
                            )
                        },
                    )?;
                    if !keep_scanning {
                        return Ok(false);
                    }
                }
            }
            BTreeNode::Uninitialized => unreachable!(),
        }
        Ok(true)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, key_evaluator, key_reader, file, vault, cache))
    )]
    pub fn get<F: ManagedFile, KeyEvaluator, KeyReader>(
        &self,
        keys: &mut KeyRange<'_>,
        key_evaluator: &mut KeyEvaluator,
        key_reader: &mut KeyReader,
        file: &mut F,
        vault: Option<&dyn Vault>,
        cache: Option<&ChunkCache>,
    ) -> Result<bool, Error>
    where
        KeyEvaluator: FnMut(&Buffer<'static>) -> KeyEvaluation,
        KeyReader: FnMut(Buffer<'static>, &I) -> Result<(), Error>,
    {
        match &self.node {
            BTreeNode::Leaf(children) => {
                let mut last_index = 0;
                while let Some(key) = keys.current_key() {
                    match children[last_index..].binary_search_by(|child| (&*child.key).cmp(key)) {
                        Ok(matching) => {
                            keys.next();
                            last_index += matching;
                            let entry = &children[last_index];
                            match key_evaluator(&entry.key) {
                                KeyEvaluation::ReadData => {
                                    key_reader(entry.key.clone(), &entry.index)?;
                                }
                                KeyEvaluation::Skip => {}
                                KeyEvaluation::Stop => return Ok(false),
                            }
                        }
                        Err(_) => {
                            // No longer matching within this tree
                            break;
                        }
                    }
                }
            }
            BTreeNode::Interior(children) => {
                let mut last_index = 0;
                while let Some(key) = keys.current_key() {
                    let containing_node_index = children[last_index..]
                        .binary_search_by(|child| (&*child.key).cmp(key))
                        .unwrap_or_else(|not_found| not_found);
                    last_index += containing_node_index;

                    // This isn't guaranteed to succeed because we add one. If
                    // the key being searched for isn't contained, it will be
                    // greater than any of the node's keys.
                    if let Some(child) = children.get(last_index) {
                        let keep_scanning = child.position.map_loaded_entry(
                            file,
                            vault,
                            cache,
                            children.len(),
                            |entry, file| {
                                entry.get(keys, key_evaluator, key_reader, file, vault, cache)
                            },
                        )?;
                        if !keep_scanning {
                            break;
                        }
                        // The leaf will consume all the keys that can match.
                        // Thus, we can skip it on the next iteration.
                        last_index += 1;
                    } else {
                        break;
                    }
                }
            }
            BTreeNode::Uninitialized => unreachable!(),
        }
        Ok(true)
    }
}

impl<
        I: Clone + BinarySerialization + Debug + 'static,
        R: Reducer<I> + Clone + BinarySerialization + Debug + 'static,
    > BinarySerialization for BTreeEntry<I, R>
{
    fn serialize_to<W: WriteBytesExt, F: ManagedFile>(
        &mut self,
        writer: &mut W,
        paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<usize, Error> {
        let mut bytes_written = 0;
        // The next byte determines the node type.
        match &mut self.node {
            BTreeNode::Leaf(leafs) => {
                debug_assert!(leafs.windows(2).all(|w| w[0].key < w[1].key));
                writer.write_u8(1)?;
                bytes_written += 1;
                for leaf in leafs {
                    bytes_written += leaf.serialize_to(writer, paged_writer)?;
                }
            }
            BTreeNode::Interior(interiors) => {
                debug_assert!(interiors.windows(2).all(|w| w[0].key < w[1].key));
                writer.write_u8(0)?;
                bytes_written += 1;
                for interior in interiors {
                    bytes_written += interior.serialize_to(writer, paged_writer)?;
                }
            }
            BTreeNode::Uninitialized => unreachable!(),
        }

        Ok(bytes_written)
    }

    fn deserialize_from(reader: &mut Buffer<'_>, current_order: usize) -> Result<Self, Error> {
        let node_header = reader.read_u8()?;
        match node_header {
            0 => {
                // Interior
                let mut nodes = Vec::new();
                nodes.reserve(current_order);
                while !reader.is_empty() {
                    nodes.push(Interior::deserialize_from(reader, current_order)?);
                }
                Ok(Self {
                    node: BTreeNode::Interior(nodes),
                    dirty: false,
                })
            }
            1 => {
                // Leaf
                let mut nodes = Vec::new();
                nodes.reserve(current_order);
                while !reader.is_empty() {
                    nodes.push(KeyEntry::deserialize_from(reader, current_order)?);
                }
                Ok(Self {
                    node: BTreeNode::Leaf(nodes),
                    dirty: false,
                })
            }
            _ => Err(Error::data_integrity("invalid node header")),
        }
    }
}

/// An operation to perform on a key.
#[derive(Debug)]
pub enum KeyOperation<T> {
    /// Do not alter the key.
    Skip,
    /// Set the key to the new value.
    Set(T),
    /// Remove the key.
    Remove,
}

struct DirectionalSliceIterator<'a, I> {
    forwards: bool,
    index: usize,
    contents: &'a [I],
}

impl<'a, I> DirectionalSliceIterator<'a, I> {
    pub const fn new(forwards: bool, contents: &'a [I]) -> Self {
        Self {
            forwards,
            contents,
            index: if forwards { 0 } else { contents.len() },
        }
    }
}

impl<'a, I> Iterator for DirectionalSliceIterator<'a, I> {
    type Item = &'a I;

    fn next(&mut self) -> Option<Self::Item> {
        if self.forwards && self.index < self.contents.len() {
            let element = &self.contents[self.index];
            self.index += 1;
            Some(element)
        } else if !self.forwards && self.index > 0 {
            self.index -= 1;
            Some(&self.contents[self.index])
        } else {
            None
        }
    }
}
