extern crate thanks;

extern crate diesel;

extern crate dotenv;

extern crate futures;

extern crate handlebars;

extern crate reqwest;

extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate slog;
extern crate slog_term;

extern crate clap;
extern crate git2;

use diesel::prelude::*;
use clap::{App, Arg};
use slog::DrainExt;

use std::collections::HashMap;
use git2::Repository;

fn main() {
    let matches = App::new("populate")
        .about("initialize the database")
        .arg(Arg::with_name("filepath")
            .short("p")
            .long("path")
            .help("filepath of the source code")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("url_path")
            .short("u")
            .long("url")
            .help("url path for this project")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("name")
            .short("n")
            .long("name")
            .help("name of the project")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("github_name")
            .short("g")
            .long("github")
            .help("GitHub link of the project")
            .takes_value(true)
            .required(true))
        .get_matches();

    let log = slog::Logger::root(slog_term::streamer().full().build().fuse(), o!("version" => env!("CARGO_PKG_VERSION")));

    let connection = thanks::establish_connection();

    // get name
    let project_name = matches.value_of("name").unwrap();
    info!(log, "Project name: {}", project_name);

    // check that we have no releases for given project
    {
        use thanks::models::Release;
        use thanks::schema::projects::dsl::*;
        use thanks::models::Project;

        if let Ok(project) = projects.filter(name.eq(project_name)).load::<Project>(&connection) {
            if let Ok(count) = Release::belonging_to(&project).count().first::<i64>(&connection) {
                if count > 0 {
                    panic!("you have releases in here already");
                }
            }
        }
    }

    // check that we have no commits
    {
        // if there are no releases then there should be no commits as well
        // so we may skip this check
        // I consider changing release_id to NOT NULL since we assign commit
        // to the first release on creation
    }

    // get path to git repo
    let path = matches.value_of("filepath").unwrap();
    info!(log, "Path to project's repo: {}", path);

    // get url path
    let url_path = matches.value_of("url_path").unwrap();
    info!(log, "URL path: {}", url_path);

    // get github name
    let github_name = matches.value_of("github_name").unwrap();
    info!(log, "GitHub name: {}", github_name);

    // create project
    let project = thanks::projects::create(&connection, project_name, url_path, github_name);

    // Create releases
    let releases = [
        // version, previous version
        ("0.2", "0.1"),
        ("0.3", "0.2"),
        ("0.4", "0.3"),
        ("0.5", "0.4"),
        ("0.6", "0.5"),
        ("0.7", "0.6"),
        ("0.8", "0.7"),
        ("0.9", "0.8"),
        ("0.10", "0.9"),
        ("0.11.0", "0.10"),
        ("0.12.0", "0.11.0"),
        ("1.0.0-alpha", "0.12.0"),
        ("1.0.0-alpha.2", "1.0.0-alpha"),
        ("1.0.0-beta", "1.0.0-alpha.2"),
        ("1.0.0", "1.0.0-beta"),
        ("1.1.0", "1.0.0"),
        ("1.2.0", "1.1.0"),
        ("1.3.0", "1.2.0"),
        ("1.4.0", "1.3.0"),
        ("1.5.0", "1.4.0"),
        ("1.6.0", "1.5.0"),
        ("1.7.0", "1.6.0"),
        ("1.8.0", "1.7.0"),
        ("1.9.0", "1.8.0"),
        ("1.10.0", "1.9.0"),
        ("1.11.0", "1.10.0"),
        ("1.12.0", "1.11.0"),
        ("1.12.1", "1.12.0"),
        ("1.13.0", "1.12.0"),
        ("1.14.0", "1.13.0"),
        ("1.15.0", "1.14.0"),
        ("1.15.1", "1.15.0"),
        ("1.16.0", "1.15.0"),
    ];

    // create 0.1, which isn't in the loop because it will have everything assigned
    // to it by default
    thanks::releases::create(&connection, "0.1", project.id, true);

    for &(release, _) in releases.iter() {
        thanks::releases::create(&connection, release, project.id, true);
    }

    // And create the release for all commits that are not released yet
    thanks::releases::create(&connection, "master", project.id, true);

    let repo = Repository::open(path).unwrap();

    let mut map = HashMap::new();

    // assign first release
    thanks::releases::assign_commits(&log, &repo, &mut map, "0.1", thanks::releases::get_first_commits(&repo, "0.1"), project.id, &path);

    // assign commits to their release
    for &(release, previous) in releases.iter() {
        thanks::releases::assign_commits(&log, &repo, &mut map, release, thanks::releases::get_commits(&repo, release, previous), project.id, &path);
    }

    // assign master
    let last = releases.last().unwrap().0;
    thanks::releases::assign_commits(&log, &repo, &mut map, "master", thanks::releases::get_commits(&repo, "master", last), project.id, &path);

    info!(log, "Done!");
}


/*fn create_commits(log: &Logger, mut map: AuthorCache, project: &Project, connection: &PgConnection, git_log: String, releases: &[(&str, &str)]) {
    use thanks::schema::releases::dsl::*;
    use thanks::models::*;


    // does this need an explicit order clause?
    let first_release = releases.
        filter(project_id.eq(project.id)).
        first::<Release>(connection).
        expect("No release found!");

    let lines = git_log.split('\n');
    let mut commits: Vec<NewCommit> = Vec::with_capacity(lines.size_hint().1.unwrap_or(lines.size_hint().0));


    info!(log, "Starting commits!");

    for log_line in lines {
        // there is a last, blank line
        if log_line == "" {
            continue;
        }

        let mut split = log_line.splitn(3, ' ');

        let sha = split.next().unwrap();
        let author_email = split.next().unwrap();
        let author_name = split.next().unwrap();

        //info!(log, "Creating commit: {}", sha);


        // We tag all commits initially to the first release. Each release will
        // set this properly below.
        let author = thanks::authors::load_or_create(&mut map, &connection, &author_name, &author_email);
        commits.push(thanks::commits::new(sha, &author, &first_release));
    }

    info!(log, "Creatd all objects!");

    use thanks::schema::commits;

    for chunk in commits.chunks(10_000) { // Postgresql limits us to (2^16 - 1) rows epr query
        diesel::insert(chunk)
            .into(commits::table)
            .execute(connection)
            .expect("Error with commit!");
    }

    info!(log, "Commits all done!")
}*/
