Since there is a difference with Sheets vs Excel, the Name Amount column requires a different function to work in Excel. The formula is (with an "=" in front):
IF(K2 = "", "",VALUE(SUBSTITUTE(TEXTAFTER(TRIM(TEXTBEFORE(D2, " " & K2, -1, 1)), " ", -1), ",", "")))

Additionally, if columns J and K do not seem to be outputing correctly, you will need to replace those formulas and refill with the following (with an "=" in front):

Column J
IFNA(XLOOKUP(TRUE, ISNUMBER(SEARCH(kw_text, D2)), kw_type), "Unmatched")

Column K
IFNA(XLOOKUP(TRUE, RIGHT(D2, LEN(name_units) + 1) = " " & name_units, name_units), "")

And if, for any reason, after the above has been resolved, formulas are not working, it may be a bug with how indentation works in Excel. 
Just remove the spaces from the formulas so that everything is on one line and redo the fill.

And after ALL OF THIS the output still seems like it isn't what it should be, the attached guide should be much easier to follow now, and much more descriptive
There could be a number of bugs, but this version should be infinitely more replicatable than the previous iteration.