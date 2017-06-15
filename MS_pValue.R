### Analyze Permutation Results
Meanings = c(
    'I', 'you', 'we', 'one', 'two', 'person', 'fish', 'dog', 'louse', 'tree', 'leaf', 'skin', 'blood', 'bone',
    'horn', 'ear', 'eye', 'nose', 'tooth', 'tongue', 'knee', 'hand', 'breast', 'liver', 'drink', 'see',
    'hear', 'die', 'come', 'sun', 'star', 'water', 'stone', 'fire', 'path', 'mountain', 'night', 'full',
    'new', 'name'
)

# Read the original SoundMeaning occurrence data
SM_table = read.table('./DataOriginal/OriginalCnt.csv', row.names = 1, header = TRUE, sep = ',')
sounds = rownames(SM_table)
row_freq = rowSums(SM_table, na.rm = TRUE)
# Read the original LanguageMeaning word length data
LM_word_length_table = read.table('./DataOriginal/LM_word_length.csv', row.names = 1, header = TRUE, sep = ',')
# Get the average num of sound locations for each meaning, and normalize it
# This will serve as the background multinomial distribution
Locus_table = LM_word_length_table - 1
Locus_table[Locus_table == -1] = 0
BG_mndist = colMeans(Locus_table); BG_mndist = BG_mndist/sum(BG_mndist)



### Function to get p-values, from permutation data
Get_pVal_1 = function(Original_row, Permute_table, BG_mndist, scale = TRUE){
    # scale: TRUE stands for scaling the test stat using BG_mndist; FALSE not
    # scale is special for Get_pVal_1
    totCnt = sum(Original_row)
    if (totCnt > 0){
        if (scale){
            n = dim(Permute_table)[1]
            expectedCnt = totCnt*BG_mndist
            expectedCntMat = matrix(1, n, 1)%*%t(expectedCnt)
            obsStat = sum((Original_row - expectedCnt)^2 / expectedCnt)
            PermStat = rowSums((Permute_table - expectedCntMat)^2 / expectedCntMat)
            return(sum(PermStat >= obsStat) / n)
        } else {
            n = dim(Permute_table)[1]
            expectedCnt = totCnt*BG_mndist
            expectedCntMat = matrix(1, n, 1)%*%t(expectedCnt)
            obsStat = sum((Original_row - expectedCnt)^2)
            PermStat = rowSums((Permute_table - expectedCntMat)^2)
            return(sum(PermStat >= obsStat) / n)
        }
    } else {
        return(NaN)
    }
}



### Compute the p-values
pVal_1 = c()
names(pVal_1) = sounds

for (i in 1:length(sounds)){
    snd = sounds[i]
    l1 = substr(snd, 1, 1)
    l2 = substr(snd, 2, 2)
    if (l1 == toupper(l1)) l1 = paste0(l1, '_')
    if (l2 == toupper(l2)) l2 = paste0(l2, '_')
    filepath = paste0('./DataPermute/', l1, l2, '.csv') ###
    Permute_table = read.table(filepath, header = TRUE, sep = ',')
    Permute_table = Permute_table[,-1] # discard the row names

    new_pVal_1 = Get_pVal_1(SM_table[i,], Permute_table, BG_mndist, scale = TRUE)
    pVal_1 = c(pVal_1, new_pVal_1)
    
}

pdf(file = "HistOfPermutationPvals.pdf", width = 8, height = 10)
par(mfrow = c(2,1))
hist(pVal_1, breaks = 20, prob = TRUE, xlab = '', main = 'Scaled Sq test stat')
hist(pVal_1[row_freq>=40], breaks = 20, prob = TRUE, xlab = '', main = 'Scaled Sq test stat (filter: tot sound cnt >= 40)')
dev.off()

