# Clean the Chandra X-ray data and export it as a dataset that's nicer to work with.
# The Chandra data is located in a file called chandra_GBS_RAW.fits

# Clean the Chandra X-ray data and export it as a dataset that's nicer to work with.
# The Chandra data is located in a file called XXX.

from astropy.io import fits
import pandas as pd
import numpy as np

def fits_to_dataframe(fp):
    """Clean Chandra X-Ray data, where the filepath (fp) is to a .fits file."""
    with fits.open(fp) as data:
        df = pd.DataFrame(data[1].data)
    print('Read in original Chandra .fits file.')
    return(df)

def rename_cols(df):
    """Rename the columns to more reasonable names.
    Add 'CX' to each GBS_NAME numeric, so that GBS_NAME reads like 'CX210' or 'CX1092'."""
    df = df.copy()
    change_dict = {'CX':'GBS_NAME','Total':'X_COUNTS','RAJ2000':'X_RAJ2000','DEJ2000':'X_DEJ2000','Dpos':'X_ERR_R'}
    df.rename(columns=change_dict,inplace=True)
    df['GBS_NAME'] = ['CX'+str(num) for num in df['GBS_NAME']]
    print('Renamed columns.')
    return(df)

def remove_cols(df):
    """Remove unnecessary columns, as shown in the list cols_2_remove. 
    Change these manually if need be."""
    df = df.copy()
    cols_2_remove = ['_RAJ2000','_DEJ2000']
    df.drop(cols_2_remove,axis=1,inplace=True)
    print('Removed unnecessary columns.')
    return(df)

def modify_x_error(df):
    """Compute the 2-sigma positional error on Chandra, using Jonker et al. 2011.
    We simply modify the current value in quadrature."""
    df = df.copy()
    df['X_ERR_R'] = [np.sqrt((P)**2.0 + (0.7)**2.0 + ((0.4085)**(-1)*0.16)**2.0) for P in df['X_ERR_R']]
    print('Computed/modified X_ERR_R using Jonker et al. 2011 correction (2-sigma error).')
    return(df)

# Filepath of Chandra X-ray Data.
fp = 'dat/chandra_GBS_RAW.fits'

# Read in the .fits file through our function which converts it to a pandas DataFrame.
df = fits_to_dataframe(fp)
df_clean = df.pipe(rename_cols).pipe(remove_cols).pipe(modify_x_error)
print('The header of the cleaned dataframe:')
print(df_clean.head())

# Output filepath of cleaned data.
fp_out = 'dat/chandra_GBS_CLEAN.csv'
df_clean.to_csv(fp_out)
print('Saved cleaned data here: {}'.format(fp_out))