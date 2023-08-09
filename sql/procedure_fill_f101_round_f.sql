create or replace procedure dm.fill_f101_round_f ( 
  i_OnDate  date
)
language plpgsql    
as $$
declare
	v_RowCount int;
begin
    call dm.writelog( '[BEGIN] fill(i_OnDate => date ''' 
         || to_char(i_OnDate, 'yyyy-mm-dd') 
         || ''');', 1
       );
    
    call dm.writelog( 'delete on_date = ' 
         || to_char(i_OnDate, 'yyyy-mm-dd'), 1
       );

    delete
      from dm.DM_F101_ROUND_F f
     where from_date = date_trunc('month', i_OnDate::timestamp)  
       and to_date = (date_trunc('MONTH', i_OnDate::timestamp) + INTERVAL '1 MONTH - 1 day');
   
    call dm.writelog('insert', 1);
   
    insert 
      into dm.dm_f101_round_f
           ( from_date         
           , to_date           
           , chapter           
           , ledger_account    
           , characteristic    
           , balance_in_rub
		   , r_balance_in_rub
           , balance_in_val 
		   , r_balance_in_val
           , balance_in_total 
		   , r_balance_in_total
           , turn_deb_rub 
		   , r_turn_deb_rub
           , turn_deb_val 
		   , r_turn_deb_val
           , turn_deb_total
		   , r_turn_deb_total
           , turn_cre_rub 
		   , r_turn_cre_rub
           , turn_cre_val
		   , r_turn_cre_val
           , turn_cre_total  
		   , r_turn_cre_total
           , balance_out_rub 
		   , r_balance_out_rub
           , balance_out_val
		   , r_balance_out_val
           , balance_out_total 
		   , r_balance_out_total
           )
with wt_turnovers as (	
	select  
		   date_trunc('month', i_OnDate::timestamp)        as from_date,
           (date_trunc('MONTH', i_OnDate::timestamp) + INTERVAL '1 MONTH - 1 day')  as to_date,
           s.chapter                             as chapter,
           substr(acc_d.account_number, 1, 5)    as ledger_account,
           acc_d.char_type                       as characteristic,
           -- RUB balance
           case 
                  when cur.currency_code in ('643', '810')
                  then b.balance_out
                  else 0
                 end                        as balance_in_rub,
          -- VAL balance converted to rub
          case 
                 when cur.currency_code not in ('643', '810')
                 then b.balance_out * exch_r.reduced_cource
                 else 0
                end                         as balance_in_val,
          -- Total: RUB balance + VAL converted to rub
          case 
                 when cur.currency_code in ('643', '810')
                 then b.balance_out
                 else b.balance_out * exch_r.reduced_cource
               end                          as balance_in_total,
           -- RUB debet turnover
           case 
                 when cur.currency_code in ('643', '810')
                 then COALESCE(at.debet_amount_rub, 0)
                 else 0
               end                          as turn_deb_rub,
           -- VAL debet turnover converted
           case 
                 when cur.currency_code not in ('643', '810')
                 then COALESCE(at.debet_amount_rub, 0)
                 else 0
               end                          as turn_deb_val,
           -- SUM = RUB debet turnover + VAL debet turnover converted
		   COALESCE(at.debet_amount_rub, 0)              as turn_deb_total,
           -- RUB credit turnover
           case 
                 when cur.currency_code in ('643', '810')
                 then COALESCE(at.credit_amount_rub, 0)
                 else 0
               end                          as turn_cre_rub,
           -- VAL credit turnover converted
           case 
                 when cur.currency_code not in ('643', '810')
                 then COALESCE(at.credit_amount_rub, 0)
                 else 0
               end                          as turn_cre_val,
           -- SUM = RUB credit turnover + VAL credit turnover converted
           COALESCE(at.credit_amount_rub, 0)            as turn_cre_total,
           
		   case 
                 when cur.currency_code in ('643', '810') and acc_d.char_type = 'A'
                 	then b.balance_out - COALESCE(at.credit_amount_rub, 0) + COALESCE(at.debet_amount_rub, 0)
			  	 when cur.currency_code in ('643', '810') and acc_d.char_type = 'P'
			   		then b.balance_out + COALESCE(at.credit_amount_rub, 0) - COALESCE(at.debet_amount_rub, 0)
                 else 0
               end								  as balance_out_rub,
			
			case 
                 when cur.currency_code not in ('643', '810') and acc_d.char_type = 'A'
                 	then (b.balance_out * exch_r.reduced_cource) - COALESCE(at.credit_amount_rub, 0) + COALESCE(at.debet_amount_rub, 0)
			  	 when cur.currency_code not in ('643', '810') and acc_d.char_type = 'P'
			   		then (b.balance_out * exch_r.reduced_cource) + COALESCE(at.credit_amount_rub, 0) - COALESCE(at.debet_amount_rub, 0)
                 else 0
               end								  as balance_out_val
			 
      from ds.md_ledger_account_s s
      join ds.md_account_d acc_d
        on substr(acc_d.account_number, 1, 5) = to_char(s.ledger_account, 'FM99999999')
      join ds.md_currency_d cur
        on cur.currency_rk = acc_d.currency_rk
      left 
      join ds.ft_balance_f b
        on b.account_rk = acc_d.account_rk
       and b.on_date  = (date_trunc('month', i_OnDate::timestamp) - INTERVAL '1 day')
      left 
      join ds.md_exchange_rate_d exch_r
        on exch_r.currency_rk = acc_d.currency_rk
       and i_OnDate between exch_r.data_actual_date and exch_r.data_actual_end_date
      left 
      join dm.dm_account_turnover_f at
        on at.account_rk = acc_d.account_rk
       and at.on_date between date_trunc('month', i_OnDate::timestamp) and (date_trunc('MONTH', i_OnDate::timestamp) + INTERVAL '1 MONTH - 1 day')
     where i_OnDate between s.start_date and s.end_date
       and i_OnDate between acc_d.data_actual_date and acc_d.data_actual_end_date
       and i_OnDate between cur.data_actual_date and cur.data_actual_end_date
	)
	select
		  t.from_date									as from_date
		, t.to_date										as to_date
		, t.chapter										as chapter
		, t.ledger_account								as ledger_account
		, t.characteristic								as characteristic
		, sum(t.balance_in_rub) 						as balance_in_rub
		, 0												as r_balance_in_rub
		, sum(t.balance_in_val) 						as balance_in_val
		, 0												as r_balance_in_val
		, sum(t.balance_in_total)						as balance_in_total
		, 0												as r_balance_in_total
		, sum(t.turn_deb_rub)							as turn_deb_rub
		, 0												as r_turn_deb_rub
		, sum(t.turn_deb_val)							as turn_deb_val
		, 0												as r_turn_deb_val
		, sum(t.turn_deb_total)							as turn_deb_total
		, 0												as r_turn_deb_total
		, sum(t.turn_cre_rub) 							as turn_cre_rub
		, 0												as r_turn_cre_rub
		, sum(t.turn_cre_val)							as turn_cre_val
		, 0												as r_turn_cre_val
		, sum(t.turn_cre_total)							as turn_cre_total
		, 0												as r_turn_cre_total
		, sum(t.balance_out_rub)						as balance_out_rub
		, 0												as r_balance_out_rub
		, sum(t.balance_out_val)						as balance_out_val
		, 0												as r_balance_out_val
		, sum(t.balance_out_rub + t.balance_out_val) 	as balance_out_total
		, 0												as r_balance_out_total
	from wt_turnovers t
	group by 
		t.from_date,
		t.to_date,
		t.chapter,
		t.ledger_account,
		t.characteristic;
	
	GET DIAGNOSTICS v_RowCount = ROW_COUNT;
    call dm.writelog('[END] inserted ' ||  to_char(v_RowCount,'FM99999999') || ' rows.', 1);

    --commit;
    
  end;$$


