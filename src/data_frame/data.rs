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
}

impl Display for Data {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        use Data::{Boolean, Date, Float, Integer, String, Vec2D, Vector};
        match self {
            String(string) => write!(f, "{}", string),
            Integer(integer) => {
                if integer.is_negative() {
                    write!(f, "-{:<10}  ", integer.abs())
                } else {
                    write!(f, " {integer:<10}  ")
                }
            }
            Float(float) => {
                if float.is_nan() {
                    write!(f, " {:<9.8}   ", "NaN")
                } else if float.is_infinite() {
                    if float.is_sign_negative() {
                        write!(f, "-{:<9.8}   ", "Infinity")
                    } else {
                        write!(f, " {:<9.8}   ", "Infinity")
                    }
                } else if *float == 0.0 {
                    write!(f, " {:<9.8}   ", "0")
                } else {
                    let exp = float.abs().log(10.0).floor() as i32;
                    let significand = float / 10_f32.powi(exp);

                    let significand_string = if float.is_sign_negative() {
                        format!("-{}", significand.abs())
                    } else {
                        format!(" {}", significand.abs())
                    };
                    if exp != 0 {
                        write!(f, "{significand_string:<9.8}e{exp: >3}")
                    } else {
                        write!(f, "{significand_string:<9.8}    ")
                    }
                }
            }
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
