use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn fixed_record_len(&self) -> usize {
        let mut sequential_offset = 0usize;
        let mut offsets = HashMap::<&str, usize>::new();
        let mut redefine_consumed = HashMap::<&str, usize>::new();
        let mut max_end = 0usize;

        for field in &self.fields {
            let len = field.byte_len();
            let offset = if let Some(target) = field.redefines.as_deref() {
                if let Some(base) = offsets.get(target).copied() {
                    let rel = redefine_consumed.get(target).copied().unwrap_or(0);
                    redefine_consumed.insert(target, rel + len);
                    base + rel
                } else {
                    let start = sequential_offset;
                    sequential_offset += len;
                    start
                }
            } else {
                let start = sequential_offset;
                sequential_offset += len;
                start
            };
            offsets.insert(&field.name, offset);
            max_end = max_end.max(offset + len);
        }

        max_end
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub level: u8,
    pub name: String,
    pub picture: Option<Picture>,
    pub occurs: usize,
    pub redefines: Option<String>,
    pub depending_on: Option<String>,
}

impl Field {
    pub fn byte_len(&self) -> usize {
        self.picture.as_ref().map_or(0, Picture::byte_len) * self.occurs.max(1)
    }

    pub fn is_leaf(&self) -> bool {
        self.picture.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Usage {
    Display,
    Comp,
    Comp3,
    Comp1,
    Comp2,
    Comp4,
    Comp5,
    Binary,
}

#[derive(Debug, Clone)]
pub struct Picture {
    pub raw: String,
    pub signed: bool,
    pub digits_before: usize,
    pub digits_after: usize,
    pub alpha_len: Option<usize>,
    pub usage: Usage,
}

impl Picture {
    pub fn byte_len(&self) -> usize {
        if let Some(alpha_len) = self.alpha_len {
            return alpha_len;
        }

        let digits = self.digits_before + self.digits_after;
        match self.usage {
            Usage::Display => digits,
            Usage::Comp3 => {
                let nibbles = digits + usize::from(self.signed);
                nibbles.div_ceil(2)
            }
            Usage::Comp1 => 4,
            Usage::Comp2 => 8,
            Usage::Comp | Usage::Comp4 | Usage::Comp5 | Usage::Binary => {
                if digits <= 4 {
                    2
                } else if digits <= 9 {
                    4
                } else {
                    8
                }
            }
        }
    }

    pub fn is_alphanumeric(&self) -> bool {
        self.alpha_len.is_some()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Text(String),
    Number(String),
    Bytes(Vec<u8>),
}

pub type Row = Vec<(String, Value)>;
