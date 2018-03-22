CREATE SCHEMA aus_gov_board;
CREATE ROLE aus_gov_board;
ALTER SCHEMA aus_gov_board OWNER TO aus_gov_board ;
GRANT USAGE ON SCHEMA aus_gov_board TO aus_gov_board_access ;
CREATE ROLE aus_gov_board_access;
GRANT aus_gov_board TO mccgr;
