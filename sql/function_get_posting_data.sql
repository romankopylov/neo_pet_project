CREATE OR REPLACE FUNCTION ds.get_posting_data(i_Date date) 
RETURNS TABLE (oper_date date, min_credit numeric, min_debet numeric, max_credit numeric, max_debet numeric) AS $$
BEGIN
  RETURN QUERY
  SELECT
    ft_posting_f.oper_date as oper_date,
    min(credit_amount) as min_credit,
    min(debet_amount) as min_debet,
    max(credit_amount) as max_credit,
    max(debet_amount) as max_debet
  FROM ds.ft_posting_f 
  WHERE ft_posting_f.oper_date = i_Date
  GROUP BY ft_posting_f.oper_date;
END;
$$ LANGUAGE plpgsql;
