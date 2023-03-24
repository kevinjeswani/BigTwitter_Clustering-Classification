# BigTwitter_Clustering-Classification
Topic modelling and sentiment analysis on big twitter data (4-55mil. tweets)
o	Streamed tweets into AWS S3 with Kinesis Firehose and combined it with a larger 55 mil.-tweet dataset *(Not covered in this repo)*
o	Utilized PySpark in DataBricks to build custom PySpark transformers, label sentiment with SparkNLP/VADER, explore SparkML RandomForest and Logistic Regression classifiers, and to perform Latent Drichlet Allocation topic modelling
o	Visualized results in AWS QuickSight through an Athena pipeline

**WeCloudData Bootcamp 2022 (Part-time Cohort)**<br> </font>
By: Kevin Jeswani & Junaid Zafar <br>
The set of notebooks are segmented for the purpose of clarity & convenience <br>
The following is the suggested order for running the scripts:
- '1_WCD_Twitter_Inflation_Classification' - Mounted S3 bucket for inflation tweets, copied over twitter data, tweet cleaning. VADER & Spark-NLP pre-trained model is used to apply labels to the inflation tweets. The data is then transformed with spark-ml. Logistic regression & random forest are built and trained with gridsearchCV on the label and transformed token features.
- '2_WCD_Twitter_AllTopics_Clustering'  **This Notebook** - All topics in the WCD twitter bucket are filtered, custom transformers are built and inserted into an extensive pipeline to load raw data from Kinesis firehose. Clustering uses Latent Dirichlet Allocation is conducted using a custom gridsearch to perform topic modelling.<br>

**Appendices** - Please note these notebooks are included simply as supporting information and to show that other experiments and exercises were conduct. Less time and effort was spent formatting on these notebooks, whereas Notebook 1) and 2) are the main submission documents.
- 'AppA_WCD_Twitter_Inflation_Classification_MLPOnly' - Experimentation for classification with multi-layer perceptron models - originally at the end of Notebook 1)
- 'AppB_WCD_Twitter_Inflation_Clustering' -Inflation tweet data with Spark-NLP labels imported, custom transformer for data cleaning built and combined with standard nlp transformers in a pipeline. LDA clustering implemented to model topics in the inflation dataset. An attemp was made with a GMM clustering model.
- 'AppC_WCD_Twitter_AllTopics_52mil_Clustering' - ALL streamed tweets (55mil+) are loaded from the WCD bucket, a transformation pipeline is built and all the data is transformed. A LDM clustering is built to cluster all the topics. 
- 'AppD_WCD_Twitter_AllTopics_Clustering_Evaluation' - An attempt was made to visualize the clustering using principal component analysis and t-SNE, but the data transformation required was too heavy to process and other issues occured. <br> 
