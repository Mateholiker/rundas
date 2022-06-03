use std::{
    collections::HashMap,
    hash::Hash,
    ops::{Deref, DerefMut},
};

use super::DataFrame;

pub struct Groups<G: Eq + Hash> {
    groups: Vec<(G, DataFrame)>,
}

impl<G: Eq + Hash> Groups<G> {
    pub(super) fn new(groups: Vec<(G, DataFrame)>) -> Groups<G> {
        Groups { groups }
    }

    ///(group lenght, number of groups with that lenght)
    pub fn distribution(&self) -> Vec<(usize, u32)> {
        let mut map = HashMap::new();
        for (_key, group) in self.groups.iter() {
            let num: &mut u32 = map.entry(group.len()).or_default();
            *num += 1;
        }
        let mut map = map.drain().collect::<Vec<_>>();
        map.sort_by_key(|(a, _b)| *a);

        map
    }

    pub fn filter<F>(&mut self, mut filter: F)
    where
        F: FnMut(&(G, DataFrame)) -> bool,
    {
        self.groups.drain_filter(|group| !filter(group));
    }
}

impl<G: Eq + Hash> Deref for Groups<G> {
    type Target = Vec<(G, DataFrame)>;

    fn deref(&self) -> &Self::Target {
        &self.groups
    }
}

impl<G: Eq + Hash> DerefMut for Groups<G> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.groups
    }
}
