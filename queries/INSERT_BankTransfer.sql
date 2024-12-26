INSERT INTO public.transactions_n26 
    (bookingdate, valuedate, partnername, partneriban, typess, paymentreference, accountname, amounteur, originalamount, originalcurrency, exchangerate)
VALUES 
    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)