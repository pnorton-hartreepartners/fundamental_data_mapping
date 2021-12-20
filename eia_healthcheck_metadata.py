from constants import TAB_DESCRIPTION, DESCRIPTION, LOCATION, PRODUCT_CODE, SOURCE_KEY


def metadata_health_check1(df):
    # confirm there is no more than one entry for every combination of these columns
    x = df.groupby([TAB_DESCRIPTION, DESCRIPTION, LOCATION]).size()
    assert x[x > 1].empty


def metadata_health_check2(df):
    # now try to understand the mismatch between product_code and description
    df.reset_index(inplace=True)

    # the only way to do this is visually
    # because there are lots of legitimate cases of single counts (in US location)
    columns = [TAB_DESCRIPTION, DESCRIPTION, PRODUCT_CODE]
    pivot_df = df.reset_index().\
        pivot_table(index=columns, columns=LOCATION, values=SOURCE_KEY, aggfunc='count')

    pivot_df.sort_values(by=columns, inplace=True)
    pivot_df.to_clipboard()


def metadata_health_check3(df):
    # where we cleaned the description
    mask = df[DESCRIPTION] != df['raw_description']
    print(df.loc[mask, [SOURCE_KEY, LOCATION, 'raw_description', DESCRIPTION]])
    df[mask].to_clipboard()
    print()
