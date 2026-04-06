#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn fixed_record_len(&self) -> usize {
        self.fields.iter().map(Field::byte_len).sum()
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub level: u8,
    pub name: String,
    pub picture: Option<Picture>,
    pub occurs: usize,
}

impl Field {
    pub fn byte_len(&self) -> usize {
        self.picture.as_ref().map_or(0, Picture::byte_len) * self.occurs.max(1)
    }

    pub fn is_leaf(&self) -> bool {
        self.picture.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct Picture {
    pub raw: String,
    pub signed: bool,
    pub digits_before: usize,
    pub digits_after: usize,
    pub alpha_len: Option<usize>,
}

impl Picture {
    pub fn byte_len(&self) -> usize {
        self.alpha_len
            .unwrap_or(self.digits_before + self.digits_after + usize::from(self.signed))
    }

    pub fn is_alphanumeric(&self) -> bool {
        self.alpha_len.is_some()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Text(String),
    Number(String),
}

pub type Row = Vec<(String, Value)>;
