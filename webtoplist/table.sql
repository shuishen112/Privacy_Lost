CREATE TABLE domain_owner(
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  parent_id INTEGER,
  name TEXT,
  aliases TEXT,
  homepage_url TEXT,
  privacy_policy_url TEXT,
  notes TEXT,
  country TEXT
);
CREATE TABLE sqlite_sequence(name, seq);
CREATE TABLE domain(
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  ip_addr TEXT,
  fqdn_md5 TEXT UNIQUE,
  fqdn TEXT,
  domain_md5 TEXT,
  domain TEXT,
  pubsuffix_md5 TEXT,
  pubsuffix TEXT,
  tld_md5 TEXT,
  tld TEXT,
  domain_owner_id INTEGER DEFAULT NULL REFERENCES domain_owner(id)
);
CREATE TABLE page(
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  browser_type TEXT,
  browser_version TEXT,
  browser_wait INTEGER,
  title TEXT,
  meta_desc TEXT,
  start_url_md5 TEXT,
  start_url TEXT,
  final_url_md5 TEXT,
  final_url TEXT,
  is_ssl BOOLEAN,
  source TEXT,
  load_time INTEGER,
  domain_id INTEGER REFERENCES domain(id),
  accessed TEXT,
  UNIQUE (accessed, start_url_md5)
);
CREATE TABLE element(
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  page_id INTEGER REFERENCES page(id),
  full_url_md5 TEXT,
  full_url TEXT,
  element_url_md5 TEXT,
  element_url TEXT,
  is_3p BOOLEAN,
  is_ssl BOOLEAN,
  received BOOLEAN,
  referer TEXT,
  page_domain_in_referer BOOLEAN,
  start_time_offset INTEGER,
  load_time INTEGER,
  status TEXT,
  status_text TEXT,
  content_type TEXT,
  body_size INTEGER,
  request_headers TEXT,
  response_headers TEXT,
  extension TEXT,
  type TEXT,
  args TEXT,
  domain_id INTEGER REFERENCES domain(id)
);
CREATE TABLE cookie(
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL REFERENCES page(id),
  page_id INTEGER,
  name TEXT,
  secure TEXT,
  path TEXT,
  domain TEXT,
  httponly TEXT,
  expiry TEXT,
  value TEXT,
  is_3p BOOLEAN,
  captured TEXT,
  domain_id INTEGER REFERENCES domain(id)
);
CREATE TABLE error(
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  url TEXT NOT NULL,
  msg TEXT NOT NULL,
  timestamp TEXT NOT NULL,
  UNIQUE (url, msg)
);