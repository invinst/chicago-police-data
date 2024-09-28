import pandas as pd
from sentence_transformers import SentenceTransformer, util
from nicknames import NickNamer
import textdistance
from metaphone import doublemetaphone

def phonetic_metaphone(name1: str, name2: str):
    metaphone_word1 = doublemetaphone(name1)
    metaphone_word2 = doublemetaphone(name2)

    # match if primary or secondary codes match
    if (metaphone_word1[0] == metaphone_word2[0]) or (metaphone_word1[1] == metaphone_word2[1]):
        return True
    else:
        return False

def is_nickname(name1: str, name2: str):
    nn = NickNamer()
    # NOTE: nicknames always returns lowercase
    return (name1.lower() in nn.nicknames_of(name2)) or (name2.lower() in nn.nicknames_of(name1))

def jaro_winkler(name1: str, name2: str):
    return textdistance.jaro_winkler(name1, name2)

def name_matching_ensemble(df, col_suffix: str, suffix_ref: str = '_ref', suffix_sup: str = '_sup'):
    """
    Ensemble approach to match names using Soundex, Jaro-Winkler, SBERT, and nameparser.

    Parameters:
    - df: pandas DataFrame
    - col_suffix: str, column suffix without _ref or _sup

    Returns:
    - pandas Series of boolean values indicating if names are a match.
    """
    col_ref = col_suffix + suffix_ref
    col_sup = col_suffix + suffix_sup

    df['metaphonic_match'] = df.apply(lambda row: phonetic_metaphone(row[col_ref, col_sup]))

    # Jaro-Winkler similarity
    jaro_threshold = 0.87
    df['jaro_score'] = df.apply(lambda row: textdistance.jaro_winkler(row[col_ref], 
                                                                      row[col_sup]), axis=1)
    df['jaro_match'] = df['jaro_score'] > jaro_threshold

    # Check nicknames using nameparser
    df['nickname_match'] = df.apply(lambda row: is_nickname(row[col_ref], row[col_sup]), axis=1)

    # ensemble: any of the above
    df['ensemble_match'] = df[['jaro_match', 'nickname_match', 'metaphonic_match']]

    return df