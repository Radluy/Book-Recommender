import pandas as pd

df = pd.read_csv('datasets/Books.csv')
print(f"shape: {df.shape}")
print(f"columns: {df.columns}")
print(f"dtypes: {df.dtypes}")
df = df.drop(['Image-URL-S', 'Image-URL-M', 'Image-URL-L'], axis=1)
print(f"head: {df.head()}")
print(f"describe: {df.describe()}")
# shape: (271360, 8)
# columns: Index(['ISBN', 'Book-Title', 'Book-Author', 'Year-Of-Publication', 'Publisher',
#        'Image-URL-S', 'Image-URL-M', 'Image-URL-L'],
#       dtype='object')
# head:          ISBN                                         Book-Title           Book-Author Year-Of-Publication                   Publisher
# 0  0195153448                                Classical Mythology    Mark P. O. Morford                2002     Oxford University Press
# 1  0002005018                                       Clara Callan  Richard Bruce Wright                2001       HarperFlamingo Canada
# 2  0060973129                               Decision in Normandy          Carlo D'Este                1991             HarperPerennial
# 3  0374157065  Flu: The Story of the Great Influenza Pandemic...      Gina Bari Kolata                1999        Farrar Straus Giroux
# 4  0393045218                             The Mummies of Urumchi       E. J. W. Barber                1999  W. W. Norton &amp; Company
# describe:               ISBN      Book-Title      Book-Author  Year-Of-Publication  Publisher
# count       271360          271360           271358               271360     271358
# unique      271360          242135           102022                  202      16807
# top     0195153448  Selected Poems  Agatha Christie                 2002  Harlequin
# freq             1              27              632                13903       7535
df = pd.read_csv('datasets/Ratings.csv')
print(f"shape: {df.shape}")
print(f"columns: {df.columns}")
print(f"dtypes: {df.dtypes}")
print(f"head: {df.head()}")
print(f"describe: {df.describe()}")
# shape: (1149780, 3)
# columns: Index(['User-ID', 'ISBN', 'Book-Rating'], dtype='object')
# dtypes: User-ID         int64
# ISBN           object
# Book-Rating     int64
# dtype: object
# head:    User-ID        ISBN  Book-Rating
# 0   276725  034545104X            0
# 1   276726  0155061224            5
# 2   276727  0446520802            0
# 3   276729  052165615X            3
# 4   276729  0521795028            6
# describe:             User-ID   Book-Rating
# count  1.149780e+06  1.149780e+06
# mean   1.403864e+05  2.866950e+00
# std    8.056228e+04  3.854184e+00
# min    2.000000e+00  0.000000e+00
# 25%    7.034500e+04  0.000000e+00
# 50%    1.410100e+05  0.000000e+00
# 75%    2.110280e+05  7.000000e+00
# max    2.788540e+05  1.000000e+01
df: pd.DataFrame = pd.read_pickle('datasets/dataset.pkl')
print(df.shape)
print(df[df['Book-Title'].str.contains('fellowship')]['Book-Title'])

print(df['ISBN'].apply(lambda x: len(x)).count())

def validate_isbn(isbn):
    sum = 0
    for idx, char in enumerate(isbn):
        if char == 'x':
            char = 10
        sum += int(char) * (10 - idx)
    return sum % 11 == 0
print(df['ISBN'].apply(lambda x: validate_isbn(x)))

print(df['Book-Rating'].unique())