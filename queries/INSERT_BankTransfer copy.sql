INSERT INTO public.transactions_n26 
    (bookingdate, valuedate, partnername, partneriban, typess, 
     paymentreference, accountname, amounteur, originalamount, 
     originalcurrency, exchangerate)
VALUES 
    (CURRENT_DATE, CURRENT_DATE, 'New Partner', 'DE89370400440532013000', 
     'Transfer', 'Payment reference', 'Main Account', 100.00, 100.00, 
     'EUR', 1.000000)