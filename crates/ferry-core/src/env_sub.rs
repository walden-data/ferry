use regex::Regex;

use crate::error::FerryError;

/// Substitute `${VAR}` and `${VAR:-default}` environment variable patterns in a string.
///
/// - `${VAR}` — required, errors if the environment variable is not set.
/// - `${VAR:-default}` — optional, uses `default` if the environment variable is not set.
///
/// Empty environment variables are treated as set (empty string is substituted).
pub fn substitute_env_vars(content: &str) -> Result<String, FerryError> {
    let re = Regex::new(r"\$\{([^}]+)\}").expect("Invalid env var regex");
    let mut errors: Vec<String> = Vec::new();

    let result = re.replace_all(content, |caps: &regex::Captures<'_>| {
        let var_expr = &caps[1];
        if let Some((var, default)) = var_expr.split_once(":-") {
            // ${VAR:-default} syntax
            std::env::var(var).unwrap_or_else(|_| default.to_string())
        } else {
            // ${VAR} syntax — required
            match std::env::var(var_expr) {
                Ok(val) => val,
                Err(_) => {
                    errors.push(format!("Missing required env var: {}", var_expr));
                    String::new() // placeholder, will be replaced on error
                }
            }
        }
    });

    if !errors.is_empty() {
        return Err(FerryError::Config(errors.join("; ")));
    }

    Ok(result.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_substitute_simple_var() {
        // SAFETY: test-only env var manipulation, single-threaded
        unsafe {
            std::env::set_var("FERRY_TEST_VAR", "hello_world");
        }
        let input = "prefix_${FERRY_TEST_VAR}_suffix";
        let result = substitute_env_vars(input).unwrap();
        assert_eq!(result, "prefix_hello_world_suffix");
        unsafe {
            std::env::remove_var("FERRY_TEST_VAR");
        }
    }

    #[test]
    fn test_substitute_missing_var() {
        // Ensure the var is not set
        unsafe {
            std::env::remove_var("FERRY_MISSING_VAR_XYZ");
        }
        let input = "${FERRY_MISSING_VAR_XYZ}";
        let result = substitute_env_vars(input);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            FerryError::Config(msg) => {
                assert!(msg.contains("FERRY_MISSING_VAR_XYZ"));
            }
            _ => panic!("Expected Config error, got {:?}", err),
        }
    }

    #[test]
    fn test_substitute_with_default() {
        unsafe {
            std::env::remove_var("FERRY_NOT_SET_VAR");
        }
        let input = "${FERRY_NOT_SET_VAR:-fallback_value}";
        let result = substitute_env_vars(input).unwrap();
        assert_eq!(result, "fallback_value");
    }

    #[test]
    fn test_substitute_multiple_vars() {
        unsafe {
            std::env::set_var("FERRY_VAR_A", "alpha");
            std::env::set_var("FERRY_VAR_B", "beta");
        }
        let input = "${FERRY_VAR_A}-${FERRY_VAR_B}";
        let result = substitute_env_vars(input).unwrap();
        assert_eq!(result, "alpha-beta");
        unsafe {
            std::env::remove_var("FERRY_VAR_A");
            std::env::remove_var("FERRY_VAR_B");
        }
    }

    #[test]
    fn test_no_substitution_needed() {
        let input = "plain string without variables";
        let result = substitute_env_vars(input).unwrap();
        assert_eq!(result, "plain string without variables");
    }

    #[test]
    fn test_substitute_with_default_and_existing_var() {
        unsafe {
            std::env::set_var("FERRY_EXISTING", "real_value");
        }
        let input = "${FERRY_EXISTING:-fallback}";
        let result = substitute_env_vars(input).unwrap();
        assert_eq!(result, "real_value");
        unsafe {
            std::env::remove_var("FERRY_EXISTING");
        }
    }

    #[test]
    fn test_substitute_empty_var_is_ok() {
        unsafe {
            std::env::set_var("FERRY_EMPTY_VAR", "");
        }
        let input = "before_${FERRY_EMPTY_VAR}_after";
        let result = substitute_env_vars(input).unwrap();
        assert_eq!(result, "before__after");
        unsafe {
            std::env::remove_var("FERRY_EMPTY_VAR");
        }
    }
}
