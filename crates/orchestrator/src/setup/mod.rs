use std::process::Command;
use std::time::Duration;

use async_std::task::sleep;
use aws_config::SdkConfig;

use crate::alerts::aws_sns::AWSSNS;
use crate::alerts::Alerts;
use crate::cli::alert::AlertValidatedArgs;
use crate::cli::cron::CronValidatedArgs;
use crate::cli::queue::QueueValidatedArgs;
use crate::cli::storage::StorageValidatedArgs;
use crate::cli::SetupCmd;
use crate::config::build_provider_config;
use crate::cron::event_bridge::AWSEventBridge;
use crate::cron::Cron;
use crate::data_storage::aws_s3::AWSS3;
use crate::data_storage::DataStorage;
use crate::queue::sqs::SqsQueue;
use crate::queue::QueueProvider as _;

#[derive(Clone)]
pub enum SetupConfig {
    AWS(SdkConfig),
}

impl std::fmt::Debug for SetupConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetupConfig::AWS(_) => write!(f, "AWS"),
        }
    }
}

impl std::fmt::Display for SetupConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetupConfig::AWS(_) => write!(f, "AWS"),
        }
    }
}

// Note: we are using println! instead of tracing::info! because telemetry is not yet initialized
// and it get initialized during the run_orchestrator function.
pub async fn setup_cloud(setup_cmd: &SetupCmd) -> color_eyre::Result<()> {
    // AWS
    println!("Setting up cloud. ⏳");
    let provider_params = setup_cmd.validate_provider_params().expect("Failed to validate provider params");
    let provider_config = build_provider_config(&provider_params).await;
    let aws_config = provider_config.get_aws_client_or_panic();
    println!("Cloud provider setup completed, Provider: {}", provider_config);

    // Queues
    println!("Setting up queues. ⏳");
    let queue_params = setup_cmd.validate_queue_params().expect("Failed to validate queue params");
    match queue_params {
        QueueValidatedArgs::AWSSQS(aws_sqs_params) => {
            let sqs = Box::new(SqsQueue::new_with_args(aws_sqs_params, aws_config));
            match sqs.setup().await {
                Ok(_) => println!("Queues setup completed ✅"),
                Err(e) => {
                    if e.to_string().contains("already exists") || e.to_string().contains("QueueAlreadyExists") {
                        println!("Queues already exist, skipping setup ✅");
                    } else {
                        return Err(e.into());
                    }
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Queues
    // println!("Setting up queues. ⏳");
    // let queue_params = setup_cmd.validate_queue_params().expect("Failed to validate queue params");
    // match queue_params {
    //     QueueValidatedArgs::AWSSQS(aws_sqs_params) => {
    //         let sqs = Box::new(SqsQueue::new_with_args(aws_sqs_params, aws_config));
    //         match sqs.setup().await {
    //             Ok(_) => println!("Queues setup completed ✅"),
    //             Err(e) => {
    //                 if e.to_string().contains("already exists") ||
    // e.to_string().contains("QueueAlreadyExists") {                     println!("Queues already
    // exist, skipping setup ✅");                 } else {
    //                     return Err(e.into());
    //                 }
    //             }
    //         }
    //     }
    // }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // Waiting for few seconds to let AWS index the queues
    println!("Waiting for AWS to index queues (20 seconds) ⏳");
    sleep(Duration::from_secs(20)).await;

    // Data Storage
    println!("Setting up data storage. ⏳");
    let data_storage_params = setup_cmd.validate_storage_params().expect("Failed to validate storage params");

    match data_storage_params {
        StorageValidatedArgs::AWSS3(aws_s3_params) => {
            let s3 = Box::new(AWSS3::new_with_args(&aws_s3_params, aws_config).await);
            match s3.setup(&StorageValidatedArgs::AWSS3(aws_s3_params.clone())).await {
                Ok(_) => println!("Data storage setup completed ✅"),
                Err(e) => {
                    if e.to_string().contains("already exists") || e.to_string().contains("BucketAlreadyExists") {
                        println!("Data storage already exists, skipping setup ✅");
                    } else {
                        return Err(e.into());
                    }
                }
            }
        }
    }

    // Cron
    println!("Setting up cron. ⏳");
    // Sleeping for few seconds to let AWS index the newly created queues to be used for setting up cron
    println!("Waiting for AWS resources to be ready (100 seconds) ⏳");
    sleep(Duration::from_secs(100)).await;

    let cron_params = setup_cmd.validate_cron_params().expect("Failed to validate cron params");
    match cron_params {
        CronValidatedArgs::AWSEventBridge(aws_event_bridge_params) => {
            let event_bridge = Box::new(AWSEventBridge::new_with_args(&aws_event_bridge_params, aws_config));
            match event_bridge.setup().await {
                Ok(_) => println!("Cron setup completed ✅"),
                Err(e) => {
                    if e.to_string().contains("already exists") || e.to_string().contains("ResourceAlreadyExists") {
                        println!("Cron resources already exist, skipping setup ✅");
                    } else {
                        return Err(e.into());
                    }
                }
            }
        }
    }

    // Alerts
    println!("Setting up alerts. ⏳");
    let alert_params = setup_cmd.validate_alert_params().expect("Failed to validate alert params");
    match alert_params {
        AlertValidatedArgs::AWSSNS(aws_sns_params) => {
            let aws_config = provider_config.get_aws_client_or_panic();
            let sns = Box::new(AWSSNS::new_with_args(&aws_sns_params, aws_config).await);
            match sns.setup().await {
                Ok(_) => println!("Alerts setup completed ✅"),
                Err(e) => {
                    if e.to_string().contains("already exists") || e.to_string().contains("TopicAlreadyExists") {
                        println!("Alert resources already exist, skipping setup ✅");
                    } else {
                        return Err(e.into());
                    }
                }
            }
        }
    }

    println!("Cloud setup fully completed ✅");
    Ok(())
}

pub async fn setup_db() -> color_eyre::Result<()> {
    // We run the js script in the folder root:
    println!("Setting up database. ⏳");

    // The output of migrate-mongo-config.js typically tells if a migration was applied or skipped
    let output = Command::new("node").arg("migrate-mongo-config.js").output()?;
    let output_str = String::from_utf8_lossy(&output.stdout);

    // Check if the output indicates migrations were already applied
    if output_str.contains("already applied") || output_str.contains("No migrations to run") {
        println!("Database migrations were already applied, skipping setup ✅");
    } else {
        println!("Database setup completed ✅");
    }

    Ok(())
}
