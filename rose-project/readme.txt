**** If you would rather run the python script directly, please reference the python_script_instructions.txt file for steps on doing that. ****

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

The Staging sheet is simply used to put the new data you want to work with into the document.
To add new data, simply:

1) copy the entire sheet of raw data from the source document with Control + A to select all data, followed by Control + C to copy.

2) go to the Staging sheet and click on the cell A1, then do Control + V to paste all the data into the sheet
    2a) if there is any data from a previous migration, you can either delete the sheet outright and make a new Staging sheet, 
    2b) or just delete all the data there with Control + A, followed by Delete or Backspace to remove the data

3) delete all the meta data rows, so that the column readers (Start Date, End Date, Slices By......) are all in row 1. 
    3a) you can do this by a Left Click on row 1 in the left (the 1, not a cell like A1), 
    3b) followed by a Shift + Left Click on the row ABOVE the column headers (should be row 10 or 11),
    3c) then Right Click the selected rows and select "Delete Rows 1-10"

4) copy the drug data without the meta data doing Control + A, Control + C

5) go to the desired Raw ____ Data sheet and do Control + A, followed by Delete or Backspace to remove the data

6) click on the cell A1 such that only the cell A1 is selected, then paste the data with Control + V

This should have properly pasted the data such that the analysis tables won't denature. If anything happens, you may need to make sure the Analysis tables have proper column allocation. 

If you find there are still issues with the above formulas or the process outlined to plug and play new data, check the provided guide. 
This sheet was entirely made using that as a tutorial and therefore can be recreated using that same guide