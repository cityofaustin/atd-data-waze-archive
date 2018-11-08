CREATE TABLE api.waze_archive(
country TEXT NOT NULL,
nThumbsUp integer NOT NULL,
city TEXT NOT NULL,
reportRating integer NOT NULL,
confidence integer NOT NULL,
reliability integer NOT NULL,
type TEXT NOT NULL,
uuid TEXT NOT NULL,
magvar TEXT NOT NULL,
subtype TEXT NOT NULL,
street TEXT NOT NULL,
reportDescription TEXT NOT NULL,
pubMillis TEXT NOT NULL,
longitude TEXT NOT NULL,
latitude TEXT NOT NULL,
PRIMARY KEY (uuid)
);