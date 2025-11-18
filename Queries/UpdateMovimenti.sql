UPDATE movimenti
SET quantita = @quantity, coeff_sfrido = @scrapPerc, um_sfrido=0, custom_1 = @remnantPerc
WHERE codice_ordine IN (@orders) AND codice_fase = 10