use models::{Commit, NewCommit};
use models::Author;
use models::Release;

use diesel;
use diesel::pg::PgConnection;
use diesel::prelude::*;

pub fn create<'a>(conn: &PgConnection, sha: &'a str, author: &Author, release: &Release) -> Commit {
    use schema::commits;

    let new_commit = new(sha, author, release);

    diesel::insert(&new_commit).into(commits::table)
        .get_result(conn)
        .expect("Error saving new commit")
}

pub fn new<'a>(sha: &'a str, author: &Author, release: &Release) -> NewCommit<'a> {
    NewCommit {
        sha: sha,
        release_id: release.id,
        author_id: author.id,
    }
}
