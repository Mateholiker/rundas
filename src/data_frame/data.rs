use chrono::{DateTime, Datelike, Local, Timelike};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Data {
    String(Box<String>),
    Integer(i32),
    Float(f32),
    Boolean(bool),
    Date(SimpleDateTime),
    Vector(Box<Vec<Data>>),
    Vec2D((f32, f32)),
}

impl Data {
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

    pub fn as_vec(&self) -> &Vec<Data> {
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

    pub fn try_as_vec2d(&self) -> Option<(f32, f32)> {
        if let Data::Vec2D(vec2d) = self {
            Some(*vec2d)
        } else {
            None
        }
    }

    pub fn try_as_integer(&self) -> Option<i32> {
        if let Data::Integer(int) = self {
            Some(*int)
        } else {
            None
        }
    }

    pub fn try_as_float(&self) -> Option<f32> {
        if let Data::Float(float) = self {
            Some(*float)
        } else {
            None
        }
    }

    pub fn try_as_boolean(&self) -> Option<bool> {
        if let Data::Boolean(boolean) = self {
            Some(*boolean)
        } else {
            None
        }
    }

    pub fn try_as_date(&self) -> Option<SimpleDateTime> {
        if let Data::Date(time_date) = self {
            Some(*time_date)
        } else {
            None
        }
    }

    pub fn try_as_vec(&self) -> Option<&Vec<Data>> {
        if let Data::Vector(ref vec) = self {
            Some(vec)
        } else {
            None
        }
    }

}

impl Display for Data {
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

impl From<String> for Data {
    fn from(string: String) -> Self {
        use Data::{Boolean, Date, Float, Integer, String, Vec2D};
        if let Ok(boolean) = bool::from_str(&string) {
            return Boolean(boolean);
        }
        if let Ok(integer) = i32::from_str(&string) {
            return Integer(integer);
        }
        if let Ok(float) = f32::from_str(&string) {
            return Float(float);
        }
        if let Ok(date) = DateTime::from_str(&string) {
            return Date(date.into());
        }
        if string.contains(' ') && string.split(' ').count() == 2 {
            let mut iter = string.split(' ').map(f32::from_str);
            if let (Some(Ok(x)), Some(Ok(y))) = (iter.next(), iter.next()) {
                return Vec2D((x, y));
            }
        }
        String(Box::new(string))
    }
}
//todo build makro for this
impl From<&i32> for Data {
    fn from(int: &i32) -> Self {
        Data::Integer(*int)
    }
}

impl From<i32> for Data {
    fn from(int: i32) -> Self {
        Data::Integer(int)
    }
}

impl From<f32> for Data {
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
