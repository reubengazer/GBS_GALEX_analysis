### Reads in: two dataframes, df1 and df2
### Returns: one dataframe df3, containing all df2 sources within error radii of any df1 sources.
# Use the variable GBS_NAME to link the sets.

import pandas as pd 
import numpy as np 

def counterparts(dfMain, dfMainRA_string, dfMainDEC_string, dfSec, dfSecRA_string, dfSecDEC_string, thresh = None, thresh_deg = False):
    """
    Find sources in dfSec that are "nearby" the sources in dfMain.
    Sources that are "nearby" to another source (within the radial distance 'thresh')
    are called COUNTERPARTS.

    INPUTS:
    - df_main (main dataframe)
    - df_sec (secondary dataframe)
    - all RA and DEC strings are column names in each df of their RA and DEC (values in DEGREES)
    - thresh is the distance threshold within which a source is a "counterpart/match"
        - thresh assumes ARCSECONDS unless if thresh_deg is flagged as True
    - thresh_deg says if the input thresh 

    OUTPUTS: 
    - df_sec_matches (sources in df_sec sufficiently close to df_main sources)

    """

    def deg_to_arc(deg):
        """Convert degree to arcsecond by multiplying by 3600."""
        return(float(deg)*3600.0)

    def extract_counterparts(ra,dec,dfRA,dfDEC,thresh=thresh):
        """
        Return the counterparts from df sufficiently close to the source at (ra,dec).
        - ra, dec (in DEGREES)
        - dfRA, dfDEC (in DEGREES)
        """
        # Eucliden coordinates of primary source and secondary dataframe.
        a , b = (ra,dec), (dfRA,dfDEC)
        # Distances between sourceMain and dfSec (deg), convert to arcseconds.
        dists = np.linalg.norm(a-b)
        dists = [deg_to_arc(dist) for dist in dists]
        # Find and return counterparts.
        nearby = dfSec.iloc[dists < thresh] 
        return(nearby)

    # Distance within which sources are classified as "counterparts".
    if thresh == None:
        thresh = 5.0 # ARCSECONDS
    # Convert to arcseconds if in degrees.
    if thresh_deg == True:
        thresh = deg_to_arc(thresh)

    # Initialize all_counterparts dataframe.
    all_counterparts = []
    # Initialize the RA and DEC columns of dfSec.
    dfSecRA, dfSecDEC = dfSec[dfSecRA_string], dfSec[dfSecDEC_string]

    # Loop through each source in dfMain and find counterparts, then append them to all_counterparts.
    for i, source in dfMain.iterrows():
        ra,dec = source[dfMainRA_string], source[dfMainDEC_string]
        # Extract counterparts.
        counterparts_i = extract_counterparts(ra,dec,dfSecRA,dfSecDEC,thresh=thresh)
        # Add GBS_NAME column indicating the CX# these sources are nearby.
        counterparts_i['GBS_NAME'] = source['GBS_NAME']
        # Append to all_counterparts.
        all_counterparts.append(counterparts_i)

    # Concatenate all components of all_counterparts into a single dataframe.
    all_counterparts = pd.concat(all_counterparts,axis=0)
    # Return the final dataframe of ALL COUNTERPARTS FROM dfSec to dfMain.
    return(all_counterparts)