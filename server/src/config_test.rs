use crate::config::ServerConfig;

#[test]
pub fn should_parse_sample_config() {
    let config_yaml = include_str!("../sample_config.yaml");
    let config = ServerConfig::from_yaml_str(config_yaml).expect("unable to parse from yaml");

    assert_eq!("local", config.env);

    assert_eq!(3, config.executor_catalog.len());
}
