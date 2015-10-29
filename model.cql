CREATE KEYSPACE IF NOT EXISTS chan
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
AND durable_writes = true;

CREATE TABLE IF NOT EXISTS chan.board (
  chan     text,
  board    text,
  title    text,
  worksafe int,
  perpage  int,
  pages    int,
  PRIMARY KEY (chan, board)
);

CREATE TABLE IF NOT EXISTS chan.thread (
  chan         text,
  board        text,
  number       int,
  PRIMARY KEY ((chan, board), number)
);

CREATE TABLE IF NOT EXISTS chan.post (
  chan          text,
  board         text,
  number        int,
  resto         int,
  sticky        int,
  closed        int,
  archived      int,
  now           text,
  time          int,
  name          text,
  trip          text,
  id            text,
  capcode       text,
  country       text,
  countryName   text,
  email         text,
  sub           text,
  com           text,
  tim           bigint,
  filename      text,
  ext           text,
  fsize         int,
  md5           text,
  w             int,
  h             int,
  tnw           int,
  tnh           int,
  fileDeleted   int,
  spoiler       int,
  customSpoiler int,
  omittedPosts  int,
  omittedImages int,
  replies       int,
  images        int,
  bumpLimit     int,
  imageLimit    int,
  lastModified  int,
  PRIMARY KEY ((chan, board), number)
);

CREATE TABLE IF NOT EXISTS chan.file (
  md5   text,
  ext   text,
  fsize int,
  data  blob,
  PRIMARY KEY (md5)
);
