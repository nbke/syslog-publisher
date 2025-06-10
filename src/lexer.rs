#[derive(Debug, PartialEq)]
pub enum Token<'t> {
    EOF,
    LessThan,
    GreaterThan,
    Minus,
    Whitespace(u32),
    Word(&'t [u8]),
}

impl<'t> Token<'t> {
    pub fn get_width(&self) -> usize {
        match self {
            Token::EOF => 0,
            Token::LessThan | Token::GreaterThan | Token::Minus => 1,
            Token::Whitespace(count) => *count as usize,
            Token::Word(w) => w.len(),
        }
    }
}

pub fn lex(input: &[u8]) -> Token {
    if input.len() == 0 {
        return Token::EOF;
    }

    match input[0] {
        b'<' => Token::LessThan,
        b'>' => Token::GreaterThan,
        b'-' => Token::Minus,
        b' ' => {
            let mut pos = 1u32;
            while let Some(c) = input.get(pos as usize) {
                if *c != b' ' { break; }
                pos += 1;
            }
            Token::Whitespace(pos)
        }
        _ => {
            let mut pos = 1u32;
            while let Some(c) = input.get(pos as usize) {
                match c {
                    b' ' | b'<' | b'>' | b'-' => break,
                    _ => pos += 1,
                }
            }
            Token::Word(&input[0..pos as usize])
        }
    }
}

pub fn lex_to_vec(input: &[u8]) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut pos = 0;
    loop {
        let tok = lex(&input[pos ..]);
        if tok == Token::EOF { break; }
        pos += tok.get_width();
        tokens.push(tok);
    }
    tokens
}
