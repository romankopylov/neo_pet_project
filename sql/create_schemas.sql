create schema if not exists DS;

create table if not exists DS.FT_BALANCE_F
(
	on_date date,
	account_rk integer,
	currency_rk integer,
	balance_out numeric(15,2),
	
	constraint ft_balance_f_pk primary key (on_date, account_rk)
);

create table if not exists DS.FT_POSTING_F
(
	oper_date date,
	credit_account_rk integer,
	debet_account_rk integer,
	credit_amount numeric(15,2),
	debet_amount numeric(15,2),
	
	constraint ft_posting_f_pk primary key (oper_date, credit_account_rk, debet_account_rk)
);

create table if not exists DS.MD_ACCOUNT_D
(
	data_actual_date date,
	data_actual_end_date date not null,
	account_rk integer,
	account_number varchar(20) not null,
	char_type varchar(1) not null,
	currency_rk integer not null,
	currency_code varchar(3) not null,
	
	constraint md_account_d_pk primary key (data_actual_date, account_rk)
);

create table if not exists DS.MD_CURRENCY_D
(
	currency_rk integer,
	data_actual_date date,
	data_actual_end_date date,
	currency_code varchar(3),
	code_iso_char varchar(3),
	
	constraint md_currency_d_pk primary key (currency_rk, data_actual_date)
);

create table if not exists DS.MD_EXCHANGE_RATE_D
(
	data_actual_date date,
	data_actual_end_date date,
	currency_rk integer,
	reduced_cource numeric,
	code_iso_num varchar(3),
	
	constraint md_exchange_rate_d_pk primary key (data_actual_date, currency_rk)
);

create table if not exists DS.MD_LEDGER_ACCOUNT_S
(
	chapter varchar(1),
	chapter_name varchar(16),
	section_number integer,
	section_name varchar(22),
	subsection_name varchar(21),
	ledger1_account integer,
	ledger1_account_name varchar(47),
	ledger_account integer,
	ledger_account_name varchar(153),
	characteristic varchar(1),
	is_resident integer,
	is_reserve integer,
	is_reserved integer,
	is_loan integer,
	is_reserved_assets integer,
	is_overdue integer,
	is_interest integer,
	pair_account varchar(5),
	start_date date,
	end_date date,
	is_rub_only integer,
	min_term varchar(1),
	min_term_measure varchar(1),
	max_term varchar(1),
	max_term_measure varchar(1),
	ledger_acc_full_name_translit varchar(1),
	is_revaluation varchar(1),
	is_correct varchar(1),
	
	constraint md_ledger_account_s_pk primary key (ledger_account, start_date)
);

create schema if not exists LOGS;

create table if not exists LOGS.LOG_DATA
(
	date_and_time timestamp,
	status varchar(15),
	description varchar(255)
);

create schema if not exists DM;

create table if not exists DM.DM_ACCOUNT_TURNOVER_F
(
	on_date date,
	account_rk integer,
	credit_amount numeric(23,8),
	credit_amount_rub numeric(23,8),
	debet_amount numeric(23,8),
	debet_amount_rub numeric(23,8)
);

create table if not exists DM.DM_F101_ROUND_F
(
	FROM_DATE date,
	TO_DATE date,
	CHAPTER varchar(1),
	LEDGER_ACCOUNT varchar(5),
	CHARACTERISTIC varchar(1),
	BALANCE_IN_RUB numeric(23,8),
	R_BALANCE_IN_RUB numeric(23,8),
	BALANCE_IN_VAL numeric(23,8),
	R_BALANCE_IN_VAL numeric(23,8),
	BALANCE_IN_TOTAL numeric(23,8),
	R_BALANCE_IN_TOTAL numeric(23,8),
	TURN_DEB_RUB numeric(23,8),
	R_TURN_DEB_RUB numeric(23,8),
	TURN_DEB_VAL numeric(23,8),
	R_TURN_DEB_VAL numeric(23,8),
	TURN_DEB_TOTAL numeric(23,8),
	R_TURN_DEB_TOTAL numeric(23,8),
	TURN_CRE_RUB numeric(23,8),
	R_TURN_CRE_RUB numeric(23,8),
	TURN_CRE_VAL numeric(23,8),
	R_TURN_CRE_VAL numeric(23,8),
	TURN_CRE_TOTAL numeric(23,8),
	R_TURN_CRE_TOTAL numeric(23,8),
	BALANCE_OUT_RUB numeric(23,8),
	R_BALANCE_OUT_RUB numeric(23,8),
	BALANCE_OUT_VAL numeric(23,8),
	R_BALANCE_OUT_VAL numeric(23,8),
	BALANCE_OUT_TOTAL numeric(23,8),
	R_BALANCE_OUT_TOTAL numeric(23,8)
);

create table if not exists DM.LG_MESSAGES
(
	record_id integer,
	date_time timestamp,
	pid integer,
	message text,
	message_type integer,
	usename varchar(25), 
	datname varchar(25), 
	client_addr varchar(25), 
	application_name varchar(50),
	backend_start timestamp
);

create sequence if not exists DM.SEQ_LG_MESSAGES
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
