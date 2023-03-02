CREATE TABLE de11an.yasa_stg_transactions (
	trans_id varchar(15),
	trans_date timestamp(0),
	amt numeric(10,2),
	card_num varchar(30),
	oper_type varchar(15),
	oper_result varchar(11),
	terminal varchar(5)
);

CREATE TABLE de11an.yasa_dwh_fact_transactions(
	trans_id varchar(15),
	trans_date timestamp(0),
	amt numeric(10,2),
	card_num varchar(30),
	oper_type varchar(15),
	oper_result varchar(11),
	terminal varchar(5) 
);


###########################################################

CREATE TABLE de11an.yasa_stg_terminals (
	terminal_id varchar(5),
	terminal_type varchar(3),
	terminal_city varchar (50),
	terminal_address varchar(200),
	update_dt date
);

CREATE TABLE de11an.yasa_stg_terminals_del (
	terminal_id varchar(5)
);

CREATE TABLE de11an.yasa_dwh_dim_terminals_hist (
	terminal_id varchar(5),
	terminal_type varchar(3),
	terminal_city varchar (50),
	terminal_address varchar(200),
	start_dt timestamp(0),
	end_dt timestamp(0),
	deleted_flg char(1),
	processed_dt timestamp(0)
);

#######################################################

CREATE TABLE de11an.yasa_stg_cards(
	card_num varchar(20),
	account_num varchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

CREATE TABLE de11an.yasa_stg_cards_del (
	card_num varchar(20)
);

CREATE TABLE de11an.yasa_dwh_dim_cards_hist (
	card_num varchar(20),
	account_num varchar(20),
	start_dt timestamp(0),
	end_dt timestamp(0),
	deleted_flg char(1)
);


########################################################

CREATE TABLE de11an.yasa_stg_accounts(
	account_num varchar(20),
	valid_to date,
	client varchar(10),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

CREATE TABLE de11an.yasa_stg_accounts_del (
	account_num varchar(20)
);

CREATE TABLE de11an.yasa_dwh_dim_accounts_hist (
	account_num varchar(20),
	valid_to date,
	client varchar(10),
	start_dt timestamp(0),
	end_dt timestamp(0),
	deleted_flg char(1)	
);

#########################################################

CREATE TABLE de11an.yasa_stg_clients(
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	create_dt timestamp(0),
	update_dt timestamp(0)
);

CREATE TABLE de11an.yasa_stg_clients_del (
	client_id varchar(10)
);

CREATE TABLE de11an.yasa_dwh_dim_clients_hist (
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	start_dt timestamp(0),
	end_dt timestamp(0),
	deleted_flg char(1)	
);

###################################################################

CREATE TABLE de11an.yasa_stg_blacklist (
	enty_dt date,
	passport_num varchar(15)	
);

CREATE TABLE de11an.yasa_dwh_fact_passport_blacklist(
	enty_dt date,
	passport_num varchar(15)
);

####################################################################

CREATE TABLE de11an.yasa_meta(
    schema_name varchar(30),
    source_name varchar(30),
    max_update_dt date
);

INSERT INTO de11an.yasa_meta (schema_name, source_name, max_update_dt) VALUES ('de11an', 'terminals', to_date('1900-01-01', 'YYYY-MM-DD'));
INSERT INTO de11an.yasa_meta (schema_name, source_name, max_update_dt) VALUES ('de11an', 'cards', to_date('1900-01-01', 'YYYY-MM-DD'));
INSERT INTO de11an.yasa_meta (schema_name, source_name, max_update_dt) VALUES ('de11an', 'accounts', to_date('1900-01-01', 'YYYY-MM-DD'));
INSERT INTO de11an.yasa_meta (schema_name, source_name, max_update_dt) VALUES ('de11an', 'clients', to_date('1900-01-01', 'YYYY-MM-DD'));
INSERT INTO de11an.yasa_meta (schema_name, source_name, max_update_dt) VALUES ('de11an', 'transactions', to_date('1900-01-01', 'YYYY-MM-DD'));
INSERT INTO de11an.yasa_meta (schema_name, source_name, max_update_dt) VALUES ('de11an', 'blacklist', to_date('1900-01-01', 'YYYY-MM-DD'));


#####################################################################

# Reports

CREATE TABLE de11an.yasa_rep_fraud(
	event_dt timestamp(0),
	passport varchar(15),
	fio varchar(200),
	phone varchar(16),
	event_type char(1),
	report_dt date
);
