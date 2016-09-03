CREATE KEYSPACE IF NOT EXISTS chan WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}  AND durable_writes = true;

CREATE TABLE IF NOT EXISTS chan.thread (
    chan text,
    board text,
    number int,
    posts set<int>,
    PRIMARY KEY ((chan, board), number)
) WITH CLUSTERING ORDER BY (number ASC);

CREATE TABLE IF NOT EXISTS chan.file_time (
    time bigint PRIMARY KEY,
    md5 text
);

CREATE TABLE IF NOT EXISTS chan.file_hash (
    md5 text PRIMARY KEY,
    time set<bigint>
);

CREATE TABLE IF NOT EXISTS chan.board (
    chan text,
    board text,
    pages int,
    perpage int,
    title text,
    worksafe int,
    PRIMARY KEY (chan, board)
) WITH CLUSTERING ORDER BY (board ASC);

CREATE TABLE IF NOT EXISTS chan.file (
    md5 text PRIMARY KEY,
    data blob,
    ext text,
    fsize int
);

CREATE TABLE IF NOT EXISTS chan.post (
    chan text,
    board text,
    number int,
    archived int,
    bumplimit int,
    capcode text,
    closed int,
    com text,
    country text,
    countryname text,
    customspoiler int,
    email text,
    ext text,
    filedeleted int,
    filename text,
    fsize int,
    h int,
    id text,
    imagelimit int,
    images int,
    lastmodified int,
    md5 text,
    name text,
    now text,
    omittedimages int,
    omittedposts int,
    replies int,
    resto int,
    spoiler int,
    sticky int,
    sub text,
    tim bigint,
    time int,
    tnh int,
    tnw int,
    trip text,
    w int,
    PRIMARY KEY ((chan, board), number)
) WITH CLUSTERING ORDER BY (number ASC);
