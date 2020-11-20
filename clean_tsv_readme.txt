Overall the biggest challenge was determining where the error tabs were and correcting those first via the column lengths.
From there I tackled the next main issue which was the inconsistencies in names. I assumed that any deviation in column 
length was due to errors in name input after reviewing the data file. If consectutive names in the same row were identical 
I assumed them to be duplicates, otherwise just longer names and relegated the "third" name as part of the last name. If 
there are any problems that occur in the future that do not meet this handling logic I implemented an exception handler 
that will print the offending row to the console for later evaluation.