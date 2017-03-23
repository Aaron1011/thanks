use models::{Author, NewAuthor};

use diesel::*;
use diesel::pg::PgConnection;
use std::collections::HashMap;
use std::io::stderr;
use std::io::Write;

pub type AuthorCache<'a, 'b> = &'a mut HashMap<(String, String), Author>;

pub fn load_or_create<'a, 'b>(cache: AuthorCache<'a, 'b>, conn: &PgConnection, author_name: &'b str, author_email: &'b str) -> Author {

    let new_author = NewAuthor {
        name: author_name,
        email: author_email
    };

    cache.entry((author_name.to_string(), author_email.to_string())).or_insert_with(|| { find_or_create(conn, &new_author).expect("Could not find or create author")}).clone()
}

pub fn find_or_create_all<'a ,'b>(cache: AuthorCache<'a, 'b>, conn: &PgConnection, new_authors: Vec<NewAuthor<'b>>)
    -> QueryResult<Vec<Author>>
{
    use schema::authors::dsl::*;
    use diesel::expression::dsl::any;
    use diesel::pg::upsert::*;

    let (names, emails): (Vec<_>, Vec<_>) = new_authors.iter()
        .map(|author| (author.name, author.email))
        .unzip();

    let iter = new_authors.into_iter();

    // This is more efficient than querying the DB for each author individually
    let (found, missing): (Vec<_>, Vec<_>) = iter.partition(|author| {
        cache.contains_key(&(author.name.to_owned(), author.email.to_owned()))
    });

    writeln!(stderr(), "Cache: {}/{}", missing.len(), missing.len() + found.len()).unwrap();

    let mut final_authors = Vec::new();
    for a in found.into_iter() {
        final_authors.push(cache.get(&(a.name.to_owned(), a.email.to_owned())).unwrap().clone());
    }

    if !missing.is_empty() {
        insert(&missing.on_conflict_do_nothing())
            .into(authors)
            .execute(conn)?;

        let db_authors: Vec<Author> = authors.filter(name.eq(any(names)))
            .filter(email.eq(any(emails)))
            .load(conn)?;

        let mut map_authors = Vec::new();
        for new_author in db_authors.into_iter() {
            map_authors.push(new_author.clone());
            let author_ref = map_authors.last().unwrap();

            cache.insert((author_ref.name.clone(), author_ref.email.clone()), new_author.clone());
        }
        map_authors.extend(final_authors.into_iter());
        return Ok(map_authors);
    }

    Ok(final_authors)
}

fn find_or_create(conn: &PgConnection, new_author: &NewAuthor) -> QueryResult<Author> {
    use schema::authors::dsl::*;
    use diesel::pg::upsert::*;

    let maybe_inserted = insert(&new_author.on_conflict_do_nothing())
        .into(authors)
        .get_result(conn)
        .optional()?;

    if let Some(author) = maybe_inserted {
        return Ok(author);
    }

    authors.filter(name.eq(new_author.name))
        .filter(email.eq(new_author.email))
        .first(conn)
}
