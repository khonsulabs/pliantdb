use std::collections::HashMap;

use bonsaidb_core::schema::{CollectionName, Schematic};

use crate::{
    database::document_tree_name,
    views::{
        view_document_map_tree_name, view_entries_tree_name, view_invalidated_docs_tree_name,
        view_omitted_docs_tree_name,
    },
    Error,
};

#[derive(Default)]
pub struct OpenTrees {
    pub trees: Vec<String>,
    pub trees_index_by_name: HashMap<String, usize>,
}

impl OpenTrees {
    pub fn open_tree(&mut self, name: &str) {
        if !self.trees_index_by_name.contains_key(name) {
            self.trees_index_by_name
                .insert(name.to_string(), self.trees.len());
            self.trees.push(name.to_string());
        }
    }

    pub fn open_trees_for_document_change(
        &mut self,
        database: &str,
        collection: &CollectionName,
        schema: &Schematic,
    ) -> Result<(), Error> {
        self.open_tree(&document_tree_name(database, collection));

        if let Some(views) = schema.views_in_collection(collection) {
            for view in views {
                let view_name = view.view_name()?;
                if view.unique() {
                    self.open_tree(&view_omitted_docs_tree_name(database, &view_name));
                    self.open_tree(&view_document_map_tree_name(database, &view_name));
                    self.open_tree(&view_entries_tree_name(database, &view_name));
                } else {
                    self.open_tree(&view_invalidated_docs_tree_name(database, &view_name));
                }
            }
        }

        Ok(())
    }
}
