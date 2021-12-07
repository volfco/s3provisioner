use std::collections::HashMap;

use std::fs;
use std::path;
use regex;
use anyhow;
use std::convert::TryFrom;
use std::process::exit;
use std::time::Duration;
use rusoto_s3::S3;
use structopt::StructOpt;
use tokio::{io, fs::File};

fn parse_input_vals(s: &str) -> anyhow::Result<(String, String, String)> {
    let re = regex::Regex::new(r"s3://(.*?)/(.*):(.*)").unwrap();

    if !re.is_match(s) {
        anyhow::bail!("malformed file inputs");
    }

    for grp in re.captures_iter(s) {
        return Ok((grp[1].to_string(), grp[2].to_string(), grp[3].to_string()))
    }

    anyhow::bail!("something broke during parsing")
}

#[derive(StructOpt, Debug, Clone)]
struct Args {
    /// Files to download and where to put them on the filesystem
    /// Format is `s3://(.*?)/(.*):(.*)`. So: s3://bucket-name/key:/os/path will download `key` from
    /// bucket `bucket-name` and place it at `/os/path`
    ///
    /// You can define the region by adding it at the end of the bucket name preceeded by an underscore.
    /// So, s3://bucket_us-west-2/... will try and request the file from `bucket` in us-west-2
    #[structopt(required = true, parse(try_from_str = parse_input_vals), number_of_values = 1)]
    action_pairs: Vec<(String, String, String)>,

    #[structopt(long = "sleep")]
    sleep: bool
}


async fn process(args: Args) -> anyhow::Result<()> {
    // sort the inputs into a hash-map, so we can go bucket by bucket to download the files
    let mut sorted_work = HashMap::new();
    for (bucket, key, dest) in args.action_pairs {
        let dest = path::PathBuf::try_from(dest)?;
        if !sorted_work.contains_key(&bucket) {
            let mut work_map = HashMap::new();
            work_map.insert(key, dest);
            sorted_work.insert(bucket, work_map);
        } else {
            sorted_work.get_mut(&bucket).unwrap().insert(key, dest);
        }
    }

    log::info!("parsed the following actions: {:?}", sorted_work);

    for (_, files) in &sorted_work {
        for (_, dest) in files {
            // make sure the destination folder/file
            let parent =  &dest.parent().unwrap();
            if !parent.exists() {
                log::warn!("'{}' does not exist", &parent.to_str().unwrap());
                match fs::create_dir_all(parent) {
                    Ok(_) => log::info!("successfully created '{}'", &parent.to_str().unwrap()),
                    Err(err) => { log::error!("unable to create '{}'. {:?}", &parent.to_str().unwrap(), err); exit(1); }
                }
            }
        }
    }

    // now, loop over the hashmap and build the rusoto client
    for (bucket, files) in sorted_work {
        let region = rusoto_core::Region::default();

        log::info!("built s3 client for region `{:?}`", &region);
        let client = rusoto_s3::S3Client::new(region);

        // loop over each file in the bucket we're to download
        for (key, dest) in files {

            log::debug!("requesting `{}`", &key);
            let mut obj = client.get_object(rusoto_s3::GetObjectRequest {
                bucket: bucket.clone(),
                key: key.clone(),
                ..Default::default()
            }).await?;

            log::info!("writing contents of s3://{}/{} to {}", &bucket, &key, &dest.to_string_lossy());

            let body = obj.body.take().expect("The object has no body");

            let mut body = body.into_async_read();
            let mut file = File::create(dest).await?;
            io::copy(&mut body, &mut file).await?;
        }
    }

    Ok(())
}

#[paw::main]
#[tokio::main]
async fn main(args: Args) -> anyhow::Result<()> {
    openssl_probe::init_ssl_cert_env_vars();

    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"));

    if let Err(error) = process(args.clone()).await {
        log::error!("{:?}", error);
        exit(1);
    }

    if args.sleep {
        loop {
            log::info!("sleeping forever");
            std::thread::sleep(Duration::from_secs(1 * 60 * 60))
        }
    }

    Ok(())

}
