pub(super) fn wildcard_matches(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let Some(star_index) = pattern.find('*') else {
        return pattern == value;
    };
    let (prefix, suffix_with_star) = pattern.split_at(star_index);
    let suffix = &suffix_with_star[1..];
    value.starts_with(prefix) && value.ends_with(suffix)
}

pub(super) fn pattern_covers(grant_pattern: &str, requested_pattern: &str) -> bool {
    grant_pattern == "*"
        || grant_pattern == requested_pattern
        || requested_pattern != "*"
            && grant_pattern.ends_with('*')
            && requested_pattern.starts_with(grant_pattern.trim_end_matches('*'))
}
