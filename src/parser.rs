#[cfg(test)]
mod tests {
    use encoding_rs::{ISO_8859_15, UTF_8};

    #[test]
    fn decode_iso8859_15() {
        let input = b"Wert\xe4nderung";
        let (val, encoding, was_replaced) = ISO_8859_15.decode(input);
        
        assert_eq!(val, "Wertänderung");
        assert_eq!(encoding.name(), "ISO-8859-15");
        assert!(!was_replaced);
    }
}
