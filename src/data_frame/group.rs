use std::{
    collections::{hash_map::Drain, HashMap},
    hash::Hash,
    ops::{Index, IndexMut},
};

use super::DataFrame;

pub struct Groups<G: Eq + Hash> {
    groups: HashMap<G, DataFrame>,
}

impl<G: Eq + Hash> Groups<G> {
    pub(super) fn new(groups: HashMap<G, DataFrame>) -> Groups<G> {
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

    pub fn filter<F>(mut self, mut filter: F) -> Groups<G>
    where
        F: FnMut((&G, &DataFrame)) -> bool,
    {
        self.groups.drain_filter(|key, group| !filter((key, group)));
        self
    }

    pub fn iter(&self) -> impl Iterator<Item = (&G, &DataFrame)> {
        self.groups.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&G, &mut DataFrame)> {
        self.groups.iter_mut()
    }

    pub fn drain(&mut self) -> Drain<'_, G, DataFrame> {
        self.groups.drain()
    }
}

impl<G: Eq + Hash> Index<&G> for Groups<G> {
    type Output = DataFrame;

    fn index(&self, index: &G) -> &Self::Output {
        self.groups.get(index).expect("index out ouf bound")
    }
}

impl<G: Eq + Hash> IndexMut<&G> for Groups<G> {
    fn index_mut(&mut self, index: &G) -> &mut Self::Output {
        self.groups.get_mut(index).expect("index out ouf bound")
    }
}

impl<G: Eq + Hash> Index<G> for Groups<G> {
    type Output = DataFrame;

    fn index(&self, index: G) -> &Self::Output {
        self.groups.get(&index).expect("index out ouf bound")
    }
}

impl<G: Eq + Hash> IndexMut<G> for Groups<G> {
    fn index_mut(&mut self, index: G) -> &mut Self::Output {
        self.groups.get_mut(&index).expect("index out ouf bound")
    }
}
