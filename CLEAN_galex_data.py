import pandas as pd
import numpy as np
import pylab as plt
import seaborn as sns

# Right and left half of Galex data.
# We queired CasJobs which holds the Galex NUV data.
# Due to the way the longitude in degrees works, we needed to query 
# all Galex points in the GBS region, which is continuous section
# with longitudes between ~ 357-360 and 0-3 degrees.
# Due to this split over 360, we need two queries for right and left halves, then we stack.

def stack_halves(right,left):
    """Stack the right and left halves of the CasJobs-queried Galex data, vertically."""
    glx = pd.concat([right,left],axis=0)
    print('Stacked Galex halfs into whole set.')
    return(glx)

def flag_na(df,remove_rows=False):
    """Raise a flag if there are NaN or None values in the dataset, and remove rows if True."""
    df = df.copy()
    n_bad = sum(np.ravel(df.isnull().values))
    print('{} bad values in the dataset.')
    if remove_rows == True:
        df.dropna()
    return(df)

def convert_glon(df):
    """Convert longitude values < 360 to negative equivalents where 360 meets 0."""
    df = df.copy()
    df.loc[df['glon']<360,'glon'].apply(lambda long: long-360)
    print('Converted longitudes < 360 to negative values.')
    return(df)

def trim(df):
    """Trim the Galex observations by removing all points out of the standard GBS region:
    We need to cut out the center horizontal chunk not included in the GBS between ~ -1 < lat < +1."""
    df = df.copy()
    len_init = len(df)
    long_max,long_min = 3.1, -3.1
    df = df[(df['glon'] < long_max) & (df['glon'] > long_min)] # cut longitude.
    df = df[(df['glat'] > 0.83) | (df['glat'] < -0.83)] # cut latitude by removing middle.
    len_fin = len(df)
    print('Trimmed Galex observations to represent the GBS area, reducing observations from {len_init} to {len_fin} lines.'.format(**locals()))
    return(df)

def rename_cols(df):
    """Rename the columns to more reasonable names."""
    df = df.copy()
    change_dict = {'nuv_mag':'NUV_MAG','nuv_magerr':'NUV_MAG_ERR','nuv_flux':'NUV_FLUX',\
    'nuv_fluxerr':'NUV_FLUX_ERR','ra':'GLX_RA','dec':'GLX_DEC','fov_radius':'DIST_2_FOV'}
    df.rename(columns=change_dict,inplace=True)
    print('Renamed columns.')
    return(df)

def remove_duplicates(df):
    """Remove actual duplicate observations by some column.
    Also remove CLOSE duplicates, observations with very close coordinates (in multiple images).
    There are MANY of this class, because the entire dataset is comprised of many non-unique sets.
    Initialize real_sources with the first observation.
    Loop through the rest of the dataset - if they aren't too close, add them to real_sources.
    (The NUV density is much lower than 1 per 10" radius around any point)."""
    
    def is_duplicate(point,df,tol=None):
        """Return a boolean TRUE or FALSE if there exists an observation in df
        that is sufficiently close to 'point'.
        - point: an observation from the Galex dataset.
        - df: a comparison set.
        - tol: distance tolerance to accept or reject a source."""
        # Set tolerance if not None.
        if tol == None: 
            tol = 0.000694444 # degrees, or 2.5 arcseconds.
        # Distance between point and all other observations in df, Euclidean 2-norm.
        distance = np.sqrt((point['GLX_RA']-df['GLX_RA'])**2.0 + (point['GLX_DEC']-df['GLX_DEC'])**2.0)
        # If any are close, return True.
        return(any([dist < tol for dist in distance]))
    
    df = df.copy()
    # Sort df by DIST_2_FOV as they have the smallest observational errors among duplicates.
    df.sort_values(by=['DIST_2_FOV'])
    # Create empty dataframe for our unduplicated observations.
    real_sources = pd.DataFrame(columns=df.columns)
    real_sources = real_sources.append(df.iloc[0])
    # Loop through data, only select those that have no duplicates within tol.
    print('Unduplicating ... this takes a few moments ...')
    for i, point in df.drop(0,axis=0).iterrows(): # don't do the 0th observation as this is the initial one!
        if is_duplicate(point,real_sources) != True: # if it's unique in the set thus far, no dupe
            real_sources = real_sources.append(point)

    print('Removed duplicate Galex observations, reducing observations from {} to {} lines.'.format(len(df),len(real_sources)))
    return(real_sources)

# Filepaths of right and left halves of the Galex data in the rough GBS region.
glx_right = pd.read_csv('dat/galex_right_half_RAW.csv')
glx_left = pd.read_csv('dat/galex_left_half_RAW.csv')
df = stack_halves(glx_right,glx_left)
df.to_csv('dat/galex_RAW.csv')

# Apply each of the cleaning functions to the dataframe and output the header.
df_clean = df.pipe(flagna).pipe(convert_glon).pipe(trim).pipe(rename_cols).pipe(remove_duplicates)
print('The header of the cleaned dataframe:')
print(df_clean.head()

# Output filepath of cleaned data.
fp_out = 'dat/galex_CLEAN.csv'
df_clean.to_csv(fp_out)
print('Saved cleaned data here: {}'.format(fp_out))
