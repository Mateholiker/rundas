use chrono::{DateTime, Datelike, Local, Timelike};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::ops::{Deref, Range};
use std::str::FromStr;

use crate::DataFrame;

pub(super) enum InnerData {
    String(Box<Range<usize>>),
    Integer(i32),
    Float(f32),
    Boolean(bool),
    Date(SimpleDateTime),
    #[allow(clippy::box_collection)]
    Vector(Box<Vec<InnerData>>),
    Vec2D((f32, f32)),
}

impl InnerData {
    pub(super) fn as_data<'df>(&self, df: &'df DataFrame) -> Data<'df> {
        match self {
            InnerData::String(range) => {
                Data::String(df.get_from_string_storage(range.deref().clone()))
            }
            InnerData::Integer(integer) => Data::Integer(*integer),
            InnerData::Float(float) => Data::Float(*float),
            InnerData::Boolean(boolean) => Data::Boolean(*boolean),
            InnerData::Date(date) => Data::Date(*date),
            InnerData::Vector(vec) => Data::Vector(
                vec.iter()
                    .map(|inner_data| inner_data.as_data(df))
                    .collect(),
            ),
            InnerData::Vec2D(tuple) => Data::Vec2D(*tuple),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Data<'df> {
    String(&'df str),
    Integer(i32),
    Float(f32),
    Boolean(bool),
    Date(SimpleDateTime),
    Vector(Vec<Data<'df>>),
    Vec2D((f32, f32)),
}

impl<'df> Data<'df> {
    pub fn as_string(&self) -> String {
        format!("{}", self)
    }

    pub fn as_integer(&self) -> i32 {
        if let Data::Integer(int) = self {
            *int
        } else {
            panic!("cannot convert {} to integer", self)
        }
    }

    pub fn as_float(&self) -> f32 {
        if let Data::Float(float) = self {
            *float
        } else {
            panic!("cannot convert {} to float", self)
        }
    }

    pub fn as_boolean(&self) -> bool {
        if let Data::Boolean(boolean) = self {
            *boolean
        } else {
            panic!("cannot convert {} to boolean", self)
        }
    }

    pub fn as_date(&self) -> SimpleDateTime {
        if let Data::Date(time_date) = self {
            *time_date
        } else {
            panic!("cannot convert {} to date", self)
        }
    }

    pub fn as_vec(&self) -> &Vec<Data<'df>> {
        if let Data::Vector(ref vec) = self {
            vec
        } else {
            panic!("cannot convert {} to Vec", self)
        }
    }

    pub fn as_vec2d(&self) -> (f32, f32) {
        if let Data::Vec2D(vec2d) = self {
            *vec2d
        } else {
            panic!("cannot convert {} to Vec2", self)
        }
    }

    pub(super) fn into_inner_data(self, string_storage: &mut String) -> InnerData {
        match self {
            Data::String(string) => {
                let start = string_storage.len();
                string_storage.push_str(string);
                let end = string_storage.len();
                InnerData::String(Box::new(start..end))
            }
            Data::Integer(integer) => InnerData::Integer(integer),
            Data::Float(float) => InnerData::Float(float),
            Data::Boolean(boolean) => InnerData::Boolean(boolean),
            Data::Date(simple_date_time) => InnerData::Date(simple_date_time),
            Data::Vector(mut vector) => InnerData::Vector(Box::new(
                vector
                    .drain(..)
                    .map(|data| data.into_inner_data(string_storage))
                    .collect(),
            )),
            Data::Vec2D(tuble) => InnerData::Vec2D(tuble),
        }
    }
}

impl<'df> Display for Data<'df> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        use Data::{Boolean, Date, Float, Integer, String, Vec2D, Vector};
        match self {
            String(string) => write!(f, "{}", string),
            Integer(integer) => write!(f, "{}", integer),
            Float(float) => write!(f, "{}", float),
            Boolean(boolean) => write!(f, "{}", boolean),
            Date(dt) => {
                write!(
                    f,
                    "{}:{}:{} on {}.{}.{}",
                    dt.hour, dt.minute, dt.second, dt.day, dt.month, dt.year
                )
            }
            Vector(vec) => {
                write!(f, "[ ")?;
                for data in vec.iter().rev().skip(1).rev() {
                    write!(f, "{}, ", data)?;
                }
                if let Some(last) = vec.last() {
                    write!(f, "{} ]", last)?;
                }
                Ok(())
            }
            Vec2D((x, y)) => write!(f, "({} | {})", x, y),
        }
    }
}

impl<'df> From<&'df str> for Data<'df> {
    fn from(string: &'df str) -> Self {
        use Data::{Boolean, Date, Float, Integer, String, Vec2D};
        if let Ok(boolean) = bool::from_str(string) {
            return Boolean(boolean);
        }
        if let Ok(integer) = i32::from_str(string) {
            return Integer(integer);
        }
        if let Ok(float) = f32::from_str(string) {
            return Float(float);
        }
        if let Ok(date) = DateTime::from_str(string) {
            return Date(date.into());
        }
        if string.contains(' ') && string.split(' ').count() == 2 {
            let mut iter = string.split(' ').map(f32::from_str);
            if let (Some(Ok(x)), Some(Ok(y))) = (iter.next(), iter.next()) {
                return Vec2D((x, y));
            }
        }
        String(string)
    }
}

//todo build makro for this
impl<'df> From<&i32> for Data<'df> {
    fn from(int: &i32) -> Self {
        Data::Integer(*int)
    }
}

impl<'df> From<i32> for Data<'df> {
    fn from(int: i32) -> Self {
        Data::Integer(int)
    }
}

impl<'df> From<f32> for Data<'df> {
    fn from(float: f32) -> Self {
        Data::Float(float)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct SimpleDateTime {
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
}

impl PartialOrd for SimpleDateTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SimpleDateTime {
    fn cmp(&self, other: &Self) -> Ordering {
        use Ordering::Equal;
        match self.year.cmp(&other.year) {
            Equal => {
                let self_array = [self.month, self.day, self.hour, self.minute, self.second];
                let other_array = [
                    other.month,
                    other.day,
                    other.hour,
                    other.minute,
                    other.second,
                ];
                self_array
                    .iter()
                    .zip(other_array.iter())
                    .map(|(s, o)| s.cmp(o))
                    .find(|cmp| *cmp != Equal)
                    .unwrap_or(Equal)
            }
            other => other,
        }
    }
}

impl From<DateTime<Local>> for SimpleDateTime {
    fn from(date_time: DateTime<Local>) -> Self {
        SimpleDateTime {
            year: date_time.year(),
            month: date_time.month() as u8,
            day: date_time.day() as u8,
            hour: date_time.hour() as u8,
            minute: date_time.minute() as u8,
            second: date_time.second() as u8,
        }
    }
}
