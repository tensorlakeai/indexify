#[cfg(test)]
mod tests {

    use std::vec;

    use crate::data_model::{
        FunctionExecutorResources,
        FunctionResources,
        GPUResources,
        HostResources,
        GPU_MODEL_NVIDIA_A10,
        GPU_MODEL_NVIDIA_A100_40GB,
    };

    #[test]
    fn test_can_handle_function_resources() {
        struct Case {
            description: &'static str,
            host_resources: HostResources,
            func_resources: FunctionResources,
            expected_can_handle: bool,
        }
        let cases = vec![
            Case {
                description: "enough resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_can_handle: true,
            },
            Case {
                description: "enough resources with exact match of node and host resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu_configs: vec![],
                },
                expected_can_handle: true,
            },
            Case {
                description: "not enough cpu",
                host_resources: HostResources {
                    cpu_ms_per_sec: 500,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 501,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_can_handle: false,
            },
            Case {
                description: "not enough memory",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 512 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_can_handle: false,
            },
            Case {
                description: "not enough disk",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 512 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_can_handle: false,
            },
            Case {
                description: "with gpu - enough resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }, GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }],
                },
                expected_can_handle: true,
            },
            Case {
                description: "with gpu - enough resources with exact match of node and host resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu_configs: vec![GPUResources {
                        count: 4,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    },
                    GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }],
                },
                expected_can_handle: true,
            },
            Case {
                description: "with gpu - enough resources with exact match of node and host resources and no gpu requested",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu_configs: vec![],
                },
                expected_can_handle: true,
            },
            Case {
                description: "with gpu - not enough count",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }],
                },
                expected_can_handle: false,
            },
            Case {
                description: "with gpu - wrong model",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }],
                },
                expected_can_handle: false,
            },
        ];
        for case in cases {
            assert_eq!(
                case.host_resources
                    .can_handle_function_resources(&case.func_resources)
                    .is_ok(),
                case.expected_can_handle,
                "case: {}",
                case.description
            );
        }
    }

    #[test]
    fn test_consume_function_resources() {
        struct Case {
            description: &'static str,
            host_resources: HostResources,
            func_resources: FunctionResources,
            expected_fe_resources: Option<FunctionExecutorResources>, // None means error expected
            expected_host_resources_after: Option<HostResources>,     /* None means no change
                                                                       * expected */
        }

        let cases = vec![
            Case {
                description: "enough resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_fe_resources: Some(FunctionExecutorResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu: None,
                }),
                expected_host_resources_after: Some(HostResources {
                    cpu_ms_per_sec: 1000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 1024 * 1024 * 1024,
                    gpu: None,
                }),
            },
            Case {
                description: "enough resources with exact match of FE and host resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu_configs: vec![],
                },
                expected_fe_resources: Some(FunctionExecutorResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu: None,
                }),
                expected_host_resources_after: Some(HostResources {
                    cpu_ms_per_sec: 0,
                    memory_bytes: 0,
                    disk_bytes: 0,
                    gpu: None,
                }),
            },
            Case {
                description: "not enough cpu",
                host_resources: HostResources {
                    cpu_ms_per_sec: 500,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 501,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_fe_resources: None, // Error expected
                expected_host_resources_after: None, // No change expected
            },
            Case {
                description: "not enough memory",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 512 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_fe_resources: None, // Error expected
                expected_host_resources_after: None, // No change expected
            },
            Case {
                description: "not enough disk",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 512 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_fe_resources: None, // Error expected
                expected_host_resources_after: None, // No change expected
            },
            Case {
                description: "with gpu - enough resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }, GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }],
                },
                expected_fe_resources: Some(FunctionExecutorResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                }),
                expected_host_resources_after: Some(HostResources {
                    cpu_ms_per_sec: 1000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                }),
            },
            Case {
                description: "with gpu - enough resources with exact match of node and host resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu_configs: vec![GPUResources {
                        count: 4,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    },
                    GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }],
                },
                expected_fe_resources: Some(FunctionExecutorResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                }),
                expected_host_resources_after: Some(HostResources {
                    cpu_ms_per_sec: 0,
                    memory_bytes: 0,
                    disk_bytes: 0,
                    gpu: Some(GPUResources {
                        count: 0,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                }),
            },
            Case {
                description: "with gpu - enough resources with exact match of node and host resources and no gpu requested",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu_configs: vec![],
                },
                expected_fe_resources: Some(FunctionExecutorResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu: None,
                }),
                expected_host_resources_after: Some(HostResources {
                    cpu_ms_per_sec: 0,
                    memory_bytes: 0,
                    disk_bytes: 0,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                }),
            },
            Case {
                description: "with gpu - not enough count",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }],
                },
                expected_fe_resources: None, // Error expected
                expected_host_resources_after: None, // No change expected
            },
            Case {
                description: "with gpu - wrong model",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }],
                },
                expected_fe_resources: None, // Error expected
                expected_host_resources_after: None, // No change expected
            },
        ];

        for mut case in cases {
            let host_resources_before = case.host_resources.clone();
            let consume_result = case
                .host_resources
                .consume_function_resources(&case.func_resources);
            if let Some(expected_fe_resources) = &case.expected_fe_resources {
                assert!(
                    consume_result.is_ok(),
                    "consume_node_resources should succeed: {}",
                    case.description
                );
                let fe_resources = consume_result.unwrap();
                assert_eq!(
                    fe_resources, *expected_fe_resources,
                    "function executor resources returned from consume_node_resources: {}",
                    case.description
                );

                assert!(case.expected_host_resources_after.is_some(),
                    "expected_host_resources_after should be Some when consume_node_resources succeeds: {}",
                    case.description
                );
                let expected_host_resources = case.expected_host_resources_after.as_ref().unwrap();
                assert_eq!(
                    &case.host_resources, expected_host_resources,
                    "host resources after consume_node_resources: {}",
                    case.description
                );
            } else {
                assert!(
                    consume_result.is_err(),
                    "consume_node_resources should fail: {}",
                    case.description
                );
                assert_eq!(
                    case.host_resources, host_resources_before,
                    "host resources should not change after failed consume_node_resources: {}",
                    case.description
                );
            }
        }
    }

    #[test]
    fn test_free_fe_resources() {
        struct Case {
            description: &'static str,
            host_resources: HostResources,
            fe_resources: FunctionExecutorResources,
            expect_success: bool,
            expected_host_resources_after: Option<HostResources>,
        }

        let cases = vec![
            Case {
                description: "free cpu/mem/disk only",
                host_resources: HostResources {
                    cpu_ms_per_sec: 1000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 1024 * 1024 * 1024,
                    gpu: None,
                },
                fe_resources: FunctionExecutorResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu: None,
                },
                expect_success: true,
                expected_host_resources_after: Some(HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                }),
            },
            Case {
                description: "free with gpu",
                host_resources: HostResources {
                    cpu_ms_per_sec: 1000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                fe_resources: FunctionExecutorResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu: Some(GPUResources {
                        count: 5,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                expect_success: true,
                expected_host_resources_after: Some(HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 6,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                }),
            },
            Case {
                description: "free with gpu, wrong model",
                host_resources: HostResources {
                    cpu_ms_per_sec: 1000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }),
                },
                fe_resources: FunctionExecutorResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                expect_success: false,
                expected_host_resources_after: None, // No change expected
            },
            Case {
                description: "free with gpu, no gpu on host",
                host_resources: HostResources {
                    cpu_ms_per_sec: 1000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 1024 * 1024 * 1024,
                    gpu: None,
                },
                fe_resources: FunctionExecutorResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                expect_success: false,
                expected_host_resources_after: None, // No change expected
            },
        ];

        for mut case in cases {
            let host_resources_before = case.host_resources.clone();
            let result = case.host_resources.free(&case.fe_resources);
            if case.expect_success {
                assert!(result.is_ok(), "free should succeed: {}", case.description);
                if let Some(expected) = &case.expected_host_resources_after {
                    assert_eq!(
                        &case.host_resources, expected,
                        "host resources after free: {}",
                        case.description
                    );
                } else {
                    assert_eq!(
                        &case.host_resources, &host_resources_before,
                        "host resources after free: {}",
                        case.description
                    );
                }
            } else {
                assert!(result.is_err(), "free should fail: {}", case.description);
            }
        }
    }
}
