use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use arc_bytes::serde::Bytes;
use async_trait::async_trait;
use futures::{future::BoxFuture, Future, FutureExt};
use serde::{Deserialize, Serialize};
#[cfg(feature = "multiuser")]
use zeroize::Zeroize;

#[cfg(feature = "multiuser")]
use crate::schema::NamedReference;
use crate::{
    document::{Document, Header, OwnedDocument},
    permissions::Permissions,
    schema::{
        self,
        view::{self, map::MappedCollectionDocument},
        Key, Map, MappedDocument, MappedValue, Schema, SchemaName, SerializedCollection,
    },
    transaction::{self, OperationResult, Transaction},
    Error,
};

/// Defines all interactions with a [`schema::Schema`], regardless of whether it is local or remote.
#[async_trait]
pub trait Connection: Send + Sync {
    /// Accesses a collection for the connected [`schema::Schema`].
    fn collection<C: schema::Collection>(&self) -> Collection<'_, Self, C>
    where
        Self: Sized,
    {
        Collection::new(self)
    }

    /// Inserts a newly created document into the connected [`schema::Schema`]
    /// for the [`Collection`] `C`. If `id` is `None` a unique id will be
    /// generated. If an id is provided and a document already exists with that
    /// id, a conflict error will be returned.
    async fn insert<C: schema::Collection, B: Into<Bytes> + Send>(
        &self,
        id: Option<u64>,
        contents: B,
    ) -> Result<Header, Error> {
        let contents = contents.into();
        let results = self
            .apply_transaction(Transaction::insert(C::collection_name(), id, contents))
            .await?;
        if let OperationResult::DocumentUpdated { header, .. } = &results[0] {
            Ok(header.clone())
        } else {
            unreachable!(
                "apply_transaction on a single insert should yield a single DocumentUpdated entry"
            )
        }
    }

    /// Updates an existing document in the connected [`schema::Schema`] for the
    /// [`Collection`] `C`. Upon success, `doc.revision` will be updated with
    /// the new revision.
    async fn update<'a, C: schema::Collection, D: Document<'a> + Send + Sync>(
        &self,
        doc: &mut D,
    ) -> Result<(), Error> {
        let results = self
            .apply_transaction(Transaction::update(
                C::collection_name(),
                <D as Deref>::deref(doc).clone(),
                doc.as_ref().to_vec(),
            ))
            .await?;
        if let Some(OperationResult::DocumentUpdated { header, .. }) = results.into_iter().next() {
            *<D as DerefMut>::deref_mut(doc) = header;
            Ok(())
        } else {
            unreachable!(
                "apply_transaction on a single update should yield a single DocumentUpdated entry"
            )
        }
    }

    /// Retrieves a stored document from [`Collection`] `C` identified by `id`.
    async fn get<C: schema::Collection>(&self, id: u64) -> Result<Option<OwnedDocument>, Error>;

    /// Retrieves all documents matching `ids`. Documents that are not found
    /// are not returned, but no error will be generated.
    async fn get_multiple<C: schema::Collection>(
        &self,
        ids: &[u64],
    ) -> Result<Vec<OwnedDocument>, Error>;

    /// Retrieves all documents within the range of `ids`. Documents that are
    /// not found are not returned, but no error will be generated. To retrieve
    /// all documents, pass in `..` for `ids`.
    async fn list<C: schema::Collection, R: Into<Range<u64>> + Send>(
        &self,
        ids: R,
        order: Sort,
        limit: Option<usize>,
    ) -> Result<Vec<OwnedDocument>, Error>;

    /// Removes a `Document` from the database.
    async fn delete<C: schema::Collection, H: Deref<Target = Header> + Send + Sync>(
        &self,
        doc: &H,
    ) -> Result<(), Error> {
        let results = self
            .apply_transaction(Transaction::delete(
                C::collection_name(),
                doc.deref().clone(),
            ))
            .await?;
        if let OperationResult::DocumentDeleted { .. } = &results[0] {
            Ok(())
        } else {
            unreachable!(
                "apply_transaction on a single update should yield a single DocumentUpdated entry"
            )
        }
    }

    /// Initializes [`View`] for [`schema::View`] `V`.
    #[must_use]
    fn view<V: schema::SerializedView>(&'_ self) -> View<'_, Self, V>
    where
        Self: Sized,
    {
        View::new(self)
    }

    /// Queries for view entries matching [`View`].
    #[must_use]
    async fn query<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<Map<V::Key, V::Value>>, Error>
    where
        Self: Sized;

    /// Queries for view entries matching [`View`] with their source documents.
    #[must_use]
    async fn query_with_docs<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedDocument<V>>, Error>
    where
        Self: Sized;

    /// Queries for view entries matching [`View`] with their source documents, deserialized.
    #[must_use]
    async fn query_with_collection_docs<V>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedCollectionDocument<V>>, Error>
    where
        V: schema::SerializedView,
        V::Collection: SerializedCollection,
        <V::Collection as SerializedCollection>::Contents: std::fmt::Debug,
        Self: Sized,
    {
        let mapped_docs = self
            .query_with_docs::<V>(key, order, limit, access_policy)
            .await?;
        let mut collection_mapped_docs = Vec::with_capacity(mapped_docs.len());
        for doc in mapped_docs {
            collection_mapped_docs.push(doc.try_into()?);
        }
        Ok(collection_mapped_docs)
    }

    /// Reduces the view entries matching [`View`].
    #[must_use]
    async fn reduce<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, Error>
    where
        Self: Sized;

    /// Reduces the view entries matching [`View`], reducing the values by each
    /// unique key.
    #[must_use]
    async fn reduce_grouped<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<V::Key, V::Value>>, Error>
    where
        Self: Sized;

    /// Deletes all of the documents associated with this view.
    #[must_use]
    async fn delete_docs<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, Error>
    where
        Self: Sized;

    /// Applies a [`Transaction`] to the [`schema::Schema`]. If any operation in the
    /// [`Transaction`] fails, none of the operations will be applied to the
    /// [`schema::Schema`].
    async fn apply_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Vec<OperationResult>, Error>;

    /// Lists executed [`Transaction`]s from this [`schema::Schema`]. By default, a maximum of
    /// 1000 entries will be returned, but that limit can be overridden by
    /// setting `result_limit`. A hard limit of 100,000 results will be
    /// returned. To begin listing after another known `transaction_id`, pass
    /// `transaction_id + 1` into `starting_id`.
    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<transaction::Executed>, Error>;

    /// Fetches the last transaction id that has been committed, if any.
    async fn last_transaction_id(&self) -> Result<Option<u64>, Error>;

    /// Compacts the entire database to reclaim unused disk space.
    ///
    /// This process is done by writing data to a new file and swapping the file
    /// once the process completes. This ensures that if a hardware failure,
    /// power outage, or crash occurs that the original collection data is left
    /// untouched.
    ///
    /// ## Errors
    ///
    /// * [`Error::Io`]: an error occurred while compacting the database.
    async fn compact(&self) -> Result<(), crate::Error>;

    /// Compacts the collection to reclaim unused disk space.
    ///
    /// This process is done by writing data to a new file and swapping the file
    /// once the process completes. This ensures that if a hardware failure,
    /// power outage, or crash occurs that the original collection data is left
    /// untouched.
    ///
    /// ## Errors
    ///
    /// * [`Error::CollectionNotFound`]: database `name` does not exist.
    /// * [`Error::Io`]: an error occurred while compacting the database.
    async fn compact_collection<C: schema::Collection>(&self) -> Result<(), crate::Error>;

    /// Compacts the key value store to reclaim unused disk space.
    ///
    /// This process is done by writing data to a new file and swapping the file
    /// once the process completes. This ensures that if a hardware failure,
    /// power outage, or crash occurs that the original collection data is left
    /// untouched.
    ///
    /// ## Errors
    ///
    /// * [`Error::Io`]: an error occurred while compacting the database.
    async fn compact_key_value_store(&self) -> Result<(), crate::Error>;
}

/// Interacts with a collection over a `Connection`.
pub struct Collection<'a, Cn, Cl> {
    connection: &'a Cn,
    _phantom: PhantomData<Cl>, // allows for extension traits to be written for collections of specific types
}

impl<'a, Cn, Cl> Collection<'a, Cn, Cl>
where
    Cn: Connection,
    Cl: schema::Collection,
{
    /// Creates a new instance using `connection`.
    pub fn new(connection: &'a Cn) -> Self {
        Self {
            connection,
            _phantom: PhantomData::default(),
        }
    }

    /// Adds a new `Document<Cl>` with the contents `item`.
    pub async fn push(
        &self,
        item: &<Cl as SerializedCollection>::Contents,
    ) -> Result<Header, crate::Error>
    where
        Cl: schema::SerializedCollection,
    {
        let contents = Cl::serialize(item)?;
        Ok(self.connection.insert::<Cl, _>(None, contents).await?)
    }

    /// Adds a new `Document<Cl>` with the given `id` and contents `item`.
    pub async fn insert(
        &self,
        id: u64,
        item: &<Cl as SerializedCollection>::Contents,
    ) -> Result<Header, crate::Error>
    where
        Cl: schema::SerializedCollection,
    {
        let contents = Cl::serialize(item)?;
        Ok(self.connection.insert::<Cl, _>(Some(id), contents).await?)
    }

    /// Retrieves a `Document<Cl>` with `id` from the connection.
    pub async fn get(&self, id: u64) -> Result<Option<OwnedDocument>, Error> {
        self.connection.get::<Cl>(id).await
    }

    /// Retrieves all documents matching `ids`. Documents that are not found
    /// are not returned, but no error will be generated.
    pub fn list<R: Into<Range<u64>> + Send>(&self, ids: R) -> List<'_, Cn, Cl, R> {
        List {
            state: ListState::Pending(Some(ListBuilder {
                collection: self,
                range: ids,
                sort: Sort::Ascending,
                limit: None,
            })),
        }
    }

    /// Removes a `Document` from the database.
    pub async fn delete<H: Deref<Target = Header> + Send + Sync>(
        &self,
        doc: &H,
    ) -> Result<(), Error> {
        self.connection.delete::<Cl, H>(doc).await
    }
}

struct ListBuilder<'a, Cn, Cl, R> {
    collection: &'a Collection<'a, Cn, Cl>,
    range: R,
    sort: Sort,
    limit: Option<usize>,
}

enum ListState<'a, Cn, Cl, R> {
    Pending(Option<ListBuilder<'a, Cn, Cl, R>>),
    Executing(BoxFuture<'a, Result<Vec<OwnedDocument>, Error>>),
}

/// Executes [`Connection::list()`] when awaited. Also offers methods to
/// customize the options for the operation.
pub struct List<'a, Cn, Cl, R> {
    state: ListState<'a, Cn, Cl, R>,
}

impl<'a, Cn, Cl, R> List<'a, Cn, Cl, R>
where
    R: Into<Range<u64>> + Send + 'a + Unpin,
{
    fn builder(&mut self) -> &mut ListBuilder<'a, Cn, Cl, R> {
        if let ListState::Pending(Some(builder)) = &mut self.state {
            builder
        } else {
            unreachable!("Attempted to use after retrieving the result")
        }
    }

    /// Queries the view in ascending order.
    pub fn ascending(mut self) -> Self {
        self.builder().sort = Sort::Ascending;
        self
    }

    /// Queries the view in descending order.
    pub fn descending(mut self) -> Self {
        self.builder().sort = Sort::Descending;
        self
    }

    /// Sets the maximum number of results to return.
    pub fn limit(mut self, maximum_results: usize) -> Self {
        self.builder().limit = Some(maximum_results);
        self
    }
}

impl<'a, Cn, Cl, R> Future for List<'a, Cn, Cl, R>
where
    Cn: Connection,
    Cl: schema::Collection,
    R: Into<Range<u64>> + Send + 'a + Unpin,
{
    type Output = Result<Vec<OwnedDocument>, Error>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match &mut self.state {
            ListState::Executing(future) => future.as_mut().poll(cx),
            ListState::Pending(builder) => {
                let ListBuilder {
                    collection,
                    range,
                    sort,
                    limit,
                } = builder.take().unwrap();

                let future = async move {
                    collection
                        .connection
                        .list::<Cl, R>(range, sort, limit)
                        .await
                }
                .boxed();

                self.state = ListState::Executing(future);
                self.poll(cx)
            }
        }
    }
}

/// Parameters to query a `schema::View`.
pub struct View<'a, Cn, V: schema::SerializedView> {
    connection: &'a Cn,

    /// Key filtering criteria.
    pub key: Option<QueryKey<V::Key>>,

    /// The view's data access policy. The default value is [`AccessPolicy::UpdateBefore`].
    pub access_policy: AccessPolicy,

    /// The sort order of the query.
    pub sort: Sort,

    /// The maximum number of results to return.
    pub limit: Option<usize>,
}

impl<'a, Cn, V> View<'a, Cn, V>
where
    V: schema::SerializedView,
    Cn: Connection,
{
    fn new(connection: &'a Cn) -> Self {
        Self {
            connection,
            key: None,
            access_policy: AccessPolicy::UpdateBefore,
            sort: Sort::Ascending,
            limit: None,
        }
    }

    /// Filters for entries in the view with `key`.
    #[must_use]
    pub fn with_key(mut self, key: V::Key) -> Self {
        self.key = Some(QueryKey::Matches(key));
        self
    }

    /// Filters for entries in the view with `keys`.
    #[must_use]
    pub fn with_keys<IntoIter: IntoIterator<Item = V::Key>>(mut self, keys: IntoIter) -> Self {
        self.key = Some(QueryKey::Multiple(keys.into_iter().collect()));
        self
    }

    /// Filters for entries in the view with the range `keys`.
    #[must_use]
    pub fn with_key_range<R: Into<Range<V::Key>>>(mut self, range: R) -> Self {
        self.key = Some(QueryKey::Range(range.into()));
        self
    }

    /// Sets the access policy for queries.
    pub fn with_access_policy(mut self, policy: AccessPolicy) -> Self {
        self.access_policy = policy;
        self
    }

    /// Queries the view in ascending order.
    pub fn ascending(mut self) -> Self {
        self.sort = Sort::Ascending;
        self
    }

    /// Queries the view in descending order.
    pub fn descending(mut self) -> Self {
        self.sort = Sort::Descending;
        self
    }

    /// Sets the maximum number of results to return.
    pub fn limit(mut self, maximum_results: usize) -> Self {
        self.limit = Some(maximum_results);
        self
    }

    /// Executes the query and retrieves the results.
    pub async fn query(self) -> Result<Vec<Map<V::Key, V::Value>>, Error> {
        self.connection
            .query::<V>(self.key, self.sort, self.limit, self.access_policy)
            .await
    }

    /// Executes the query and retrieves the results with the associated [`Document`s](crate::document::OwnedDocument).
    pub async fn query_with_docs(self) -> Result<Vec<MappedDocument<V>>, Error> {
        self.connection
            .query_with_docs::<V>(self.key, self.sort, self.limit, self.access_policy)
            .await
    }

    /// Executes the query and retrieves the results with the associated [`CollectionDocument`s](crate::schema::CollectionDocument).
    pub async fn query_with_collection_docs(self) -> Result<Vec<MappedCollectionDocument<V>>, Error>
    where
        V::Collection: SerializedCollection,
        <V::Collection as SerializedCollection>::Contents: std::fmt::Debug,
    {
        self.connection
            .query_with_collection_docs::<V>(self.key, self.sort, self.limit, self.access_policy)
            .await
    }

    /// Executes a reduce over the results of the query
    pub async fn reduce(self) -> Result<V::Value, Error> {
        self.connection
            .reduce::<V>(self.key, self.access_policy)
            .await
    }

    /// Executes a reduce over the results of the query
    pub async fn reduce_grouped(self) -> Result<Vec<MappedValue<V::Key, V::Value>>, Error> {
        self.connection
            .reduce_grouped::<V>(self.key, self.access_policy)
            .await
    }

    /// Deletes all of the associated documents that match this view query.
    pub async fn delete_docs(self) -> Result<u64, Error> {
        self.connection
            .delete_docs::<V>(self.key, self.access_policy)
            .await
    }
}

/// A sort order.
#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub enum Sort {
    /// Sort ascending (A -> Z).
    Ascending,
    /// Sort descending (Z -> A).
    Descending,
}

/// Filters a [`View`] by key.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum QueryKey<K> {
    /// Matches all entries with the key provided.
    Matches(K),

    /// Matches all entires with keys in the range provided.
    Range(Range<K>),

    /// Matches all entries that have keys that are included in the set provided.
    Multiple(Vec<K>),
}

#[allow(clippy::use_self)] // clippy is wrong, Self is different because of generic parameters
impl<K: for<'a> Key<'a>> QueryKey<K> {
    /// Converts this key to a serialized format using the [`Key`] trait.
    pub fn serialized(&self) -> Result<QueryKey<Bytes>, Error> {
        match self {
            Self::Matches(key) => key
                .as_big_endian_bytes()
                .map_err(|err| Error::Database(view::Error::key_serialization(err).to_string()))
                .map(|v| QueryKey::Matches(Bytes::from(v.to_vec()))),
            Self::Range(range) => Ok(QueryKey::Range(range.as_big_endian_bytes().map_err(
                |err| Error::Database(view::Error::key_serialization(err).to_string()),
            )?)),
            Self::Multiple(keys) => {
                let keys = keys
                    .iter()
                    .map(|key| {
                        key.as_big_endian_bytes()
                            .map(|key| Bytes::from(key.to_vec()))
                            .map_err(|err| {
                                Error::Database(view::Error::key_serialization(err).to_string())
                            })
                    })
                    .collect::<Result<Vec<_>, Error>>()?;

                Ok(QueryKey::Multiple(keys))
            }
        }
    }
}

#[allow(clippy::use_self)] // clippy is wrong, Self is different because of generic parameters
impl<'a, T> QueryKey<T>
where
    T: AsRef<[u8]>,
{
    /// Deserializes the bytes into `K` via the [`Key`] trait.
    pub fn deserialized<K: for<'k> Key<'k>>(&self) -> Result<QueryKey<K>, Error> {
        match self {
            Self::Matches(key) => K::from_big_endian_bytes(key.as_ref())
                .map_err(|err| Error::Database(view::Error::key_serialization(err).to_string()))
                .map(QueryKey::Matches),
            Self::Range(range) => Ok(QueryKey::Range(range.deserialize().map_err(|err| {
                Error::Database(view::Error::key_serialization(err).to_string())
            })?)),
            Self::Multiple(keys) => {
                let keys = keys
                    .iter()
                    .map(|key| {
                        K::from_big_endian_bytes(key.as_ref()).map_err(|err| {
                            Error::Database(view::Error::key_serialization(err).to_string())
                        })
                    })
                    .collect::<Result<Vec<_>, Error>>()?;

                Ok(QueryKey::Multiple(keys))
            }
        }
    }
}

/// A range type that can represent all std range types and be serialized.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq)]
pub struct Range<T> {
    /// The start of the range.
    pub start: Bound<T>,
    /// The end of the range.
    pub end: Bound<T>,
}

/// A range bound that can be serialized.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq)]
pub enum Bound<T> {
    /// No bound.
    Unbounded,
    /// Bounded by the contained value (inclusive).
    Included(T),
    /// Bounded by the contained value (exclusive).
    Excluded(T),
}

impl<T> Range<T> {
    /// Maps each contained value with the function provided.
    pub fn map<U, F: Fn(T) -> U>(self, map: F) -> Range<U> {
        Range {
            start: self.start.map(&map),
            end: self.end.map(&map),
        }
    }

    /// Maps each contained value as a reference.
    pub fn map_ref<U: ?Sized, F: Fn(&T) -> &U>(&self, map: F) -> Range<&U> {
        Range {
            start: self.start.map_ref(&map),
            end: self.end.map_ref(&map),
        }
    }
}

impl<'a, T: Key<'a>> Range<T> {
    /// Serializes the range's contained values to big-endian bytes.
    pub fn as_big_endian_bytes(&'a self) -> Result<Range<Bytes>, T::Error> {
        Ok(Range {
            start: self.start.as_big_endian_bytes()?,
            end: self.end.as_big_endian_bytes()?,
        })
    }
}

impl<'a, B> Range<B>
where
    B: AsRef<[u8]>,
{
    /// Deserializes the range's contained values from big-endian bytes.
    pub fn deserialize<T: for<'k> Key<'k>>(&'a self) -> Result<Range<T>, <T as Key<'_>>::Error> {
        Ok(Range {
            start: self.start.deserialize()?,
            end: self.start.deserialize()?,
        })
    }
}

impl<T> Bound<T> {
    /// Maps the contained value, if any, and returns the resulting `Bound`.
    pub fn map<U, F: Fn(T) -> U>(self, map: F) -> Bound<U> {
        match self {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(value) => Bound::Included(map(value)),
            Bound::Excluded(value) => Bound::Excluded(map(value)),
        }
    }

    /// Maps each contained value as a reference.
    pub fn map_ref<U: ?Sized, F: Fn(&T) -> &U>(&self, map: F) -> Bound<&U> {
        match self {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(value) => Bound::Included(map(value)),
            Bound::Excluded(value) => Bound::Excluded(map(value)),
        }
    }
}

impl<'a, T: Key<'a>> Bound<T> {
    /// Serializes the contained value to big-endian bytes.
    pub fn as_big_endian_bytes(&'a self) -> Result<Bound<Bytes>, T::Error> {
        match self {
            Bound::Unbounded => Ok(Bound::Unbounded),
            Bound::Included(value) => Ok(Bound::Included(Bytes::from(
                value.as_big_endian_bytes()?.to_vec(),
            ))),
            Bound::Excluded(value) => Ok(Bound::Excluded(Bytes::from(
                value.as_big_endian_bytes()?.to_vec(),
            ))),
        }
    }
}

impl<'a, B> Bound<B>
where
    B: AsRef<[u8]>,
{
    /// Deserializes the bound's contained value from big-endian bytes.
    pub fn deserialize<T: for<'k> Key<'k>>(&'a self) -> Result<Bound<T>, <T as Key<'_>>::Error> {
        match self {
            Bound::Unbounded => Ok(Bound::Unbounded),
            Bound::Included(value) => {
                Ok(Bound::Included(T::from_big_endian_bytes(value.as_ref())?))
            }
            Bound::Excluded(value) => {
                Ok(Bound::Excluded(T::from_big_endian_bytes(value.as_ref())?))
            }
        }
    }
}

impl<T> std::ops::RangeBounds<T> for Range<T> {
    fn start_bound(&self) -> std::ops::Bound<&T> {
        std::ops::Bound::from(&self.start)
    }

    fn end_bound(&self) -> std::ops::Bound<&T> {
        std::ops::Bound::from(&self.end)
    }
}

impl<'a, T> From<&'a Bound<T>> for std::ops::Bound<&'a T> {
    fn from(bound: &'a Bound<T>) -> Self {
        match bound {
            Bound::Unbounded => std::ops::Bound::Unbounded,
            Bound::Included(value) => std::ops::Bound::Included(value),
            Bound::Excluded(value) => std::ops::Bound::Excluded(value),
        }
    }
}

impl<T> From<std::ops::Range<T>> for Range<T> {
    fn from(range: std::ops::Range<T>) -> Self {
        Self {
            start: Bound::Included(range.start),
            end: Bound::Excluded(range.end),
        }
    }
}

impl<T> From<std::ops::RangeFrom<T>> for Range<T> {
    fn from(range: std::ops::RangeFrom<T>) -> Self {
        Self {
            start: Bound::Included(range.start),
            end: Bound::Unbounded,
        }
    }
}

impl<T> From<std::ops::RangeTo<T>> for Range<T> {
    fn from(range: std::ops::RangeTo<T>) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Excluded(range.end),
        }
    }
}

impl<T: Clone> From<std::ops::RangeInclusive<T>> for Range<T> {
    fn from(range: std::ops::RangeInclusive<T>) -> Self {
        Self {
            start: Bound::Included(range.start().clone()),
            end: Bound::Included(range.end().clone()),
        }
    }
}

impl<T> From<std::ops::RangeToInclusive<T>> for Range<T> {
    fn from(range: std::ops::RangeToInclusive<T>) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Included(range.end),
        }
    }
}

impl<T> From<std::ops::RangeFull> for Range<T> {
    fn from(_: std::ops::RangeFull) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

/// Changes how the view's outdated data will be treated.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum AccessPolicy {
    /// Update any changed documents before returning a response.
    UpdateBefore,

    /// Return the results, which may be out-of-date, and start an update job in
    /// the background. This pattern is useful when you want to ensure you
    /// provide consistent response times while ensuring the database is
    /// updating in the background.
    UpdateAfter,

    /// Returns the results, which may be out-of-date, and do not start any
    /// background jobs. This mode is useful if you're using a view as a cache
    /// and have a background process that is responsible for controlling when
    /// data is refreshed and updated. While the default `UpdateBefore`
    /// shouldn't have much overhead, this option removes all overhead related
    /// to view updating from the query.
    NoUpdate,
}

/// Functions for interacting with a multi-database `BonsaiDb` instance.
#[async_trait]
pub trait StorageConnection: Send + Sync {
    /// The type that represents a database for this implementation.
    type Database: Connection;

    /// Creates a database named `name` with the `Schema` provided.
    ///
    /// ## Errors
    ///
    /// * [`Error::InvalidDatabaseName`]: `name` must begin with an alphanumeric
    ///   character (`[a-zA-Z0-9]`), and all remaining characters must be
    ///   alphanumeric, a period (`.`), or a hyphen (`-`).
    /// * [`Error::DatabaseNameAlreadyTaken`]: `name` was already used for a
    ///   previous database name. Database names are case insensitive. Returned
    ///   if `only_if_needed` is false.
    async fn create_database<DB: Schema>(
        &self,
        name: &str,
        only_if_needed: bool,
    ) -> Result<(), crate::Error> {
        self.create_database_with_schema(name, DB::schema_name(), only_if_needed)
            .await
    }

    /// Returns a reference to database `name` with schema `DB`.
    async fn database<DB: Schema>(&self, name: &str) -> Result<Self::Database, crate::Error>;

    /// Creates a database named `name` using the [`SchemaName`] `schema`.
    ///
    /// ## Errors
    ///
    /// * [`Error::InvalidDatabaseName`]: `name` must begin with an alphanumeric
    ///   character (`[a-zA-Z0-9]`), and all remaining characters must be
    ///   alphanumeric, a period (`.`), or a hyphen (`-`).
    /// * [`Error::DatabaseNameAlreadyTaken`]: `name` was already used for a
    ///   previous database name. Database names are case insensitive. Returned
    ///   if `only_if_needed` is false.
    async fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
        only_if_needed: bool,
    ) -> Result<(), crate::Error>;

    /// Deletes a database named `name`.
    ///
    /// ## Errors
    ///
    /// * [`Error::DatabaseNotFound`]: database `name` does not exist.
    /// * [`Error::Io`]: an error occurred while deleting files.
    async fn delete_database(&self, name: &str) -> Result<(), crate::Error>;

    /// Lists the databases in this storage.
    async fn list_databases(&self) -> Result<Vec<Database>, crate::Error>;

    /// Lists the [`SchemaName`]s registered with this storage.
    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, crate::Error>;

    /// Creates a user.
    #[cfg(feature = "multiuser")]
    async fn create_user(&self, username: &str) -> Result<u64, crate::Error>;

    /// Sets a user's password.
    #[cfg(feature = "password-hashing")]
    async fn set_user_password<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        password: SensitiveString,
    ) -> Result<(), crate::Error>;

    /// Authenticates as a user with a authentication method.
    #[cfg(all(feature = "multiuser", feature = "password-hashing"))]
    async fn authenticate<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        authentication: Authentication,
    ) -> Result<Authenticated, crate::Error>;

    /// Adds a user to a permission group.
    #[cfg(feature = "multiuser")]
    async fn add_permission_group_to_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), crate::Error>;

    /// Removes a user from a permission group.
    #[cfg(feature = "multiuser")]
    async fn remove_permission_group_from_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), crate::Error>;

    /// Adds a user to a permission group.
    #[cfg(feature = "multiuser")]
    async fn add_role_to_user<
        'user,
        'role,
        U: Into<NamedReference<'user>> + Send + Sync,
        R: Into<NamedReference<'role>> + Send + Sync,
    >(
        &self,
        user: U,
        role: R,
    ) -> Result<(), crate::Error>;

    /// Removes a user from a permission group.
    #[cfg(feature = "multiuser")]
    async fn remove_role_from_user<
        'user,
        'role,
        U: Into<NamedReference<'user>> + Send + Sync,
        R: Into<NamedReference<'role>> + Send + Sync,
    >(
        &self,
        user: U,
        role: R,
    ) -> Result<(), crate::Error>;
}

/// A database stored in `BonsaiDb`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Database {
    /// The name of the database.
    pub name: String,
    /// The schema defining the database.
    pub schema: SchemaName,
}

/// A plain-text password. This struct automatically overwrites the password
/// with zeroes when dropped.
#[cfg(feature = "multiuser")]
#[derive(Clone, Serialize, Deserialize, Zeroize)]
#[zeroize(drop)]
#[serde(transparent)]
pub struct SensitiveString(pub String);

#[cfg(feature = "multiuser")]
impl std::fmt::Debug for SensitiveString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SensitiveString(...)")
    }
}

#[cfg(feature = "multiuser")]
impl Deref for SensitiveString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// User authentication methods.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Authentication {
    /// Authenticate using a password.
    #[cfg(feature = "password-hashing")]
    Password(crate::connection::SensitiveString),
}

/// Information about the authenticated session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Authenticated {
    /// The user id logged in as.
    pub user_id: u64,
    /// The effective permissions granted.
    pub permissions: Permissions,
}
