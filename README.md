# **Project Description**

This project focuses on analyzing and processing Wikipedia data to uncover insights about the relationships between pages through mutual links and connected components. By leveraging PySpark for distributed data processing, the project efficiently handles large datasets to perform the following tasks:

**Goals**

- Identify Mutual Links: Extract pairs of pages that mutually link to each other, indicating a bidirectional relationship.

- Compute Connected Components: Analyze the mutual links to find clusters of interconnected pages, forming "connected components" that represent groups of related Wikipedia pages.

# **Entrypoint**:

The main entry point is **entrypoint.py**, which:

- Initializes a PySpark session.
- Loads data from Parquet files: linktarget, page, redirect, and pagelinks.
- Creates a DataFrame of unique mutual links.
- Computes bidirectional connected components from the mutual links.
- Outputs the results:
  
  - mutual_links: Contains the mutual links.
  
  - wikipedia_components: Contains the connected components.

**How to Run**

- Set Up the Environment:

- Ensure you have Python installed.
- Install PySpark if not already installed

**Run the Script**:
-Place your "entrypoint and python code file" location in EMR Step and Set up your CS535_S3_WORKSPACE
script using Python:
```python
python start_emr.py
```

# **Testing**:

**Synthetic Dataset**

The synthetic dataset is designed to comprehensively test scenarios involving mutual links and connected components in a Wikipedia-like data structure. The dataset includes cases that cover various types of page relationships, ensuring robust evaluation of the system's ability to process and analyze links, redirects, and non-existent pages. Below are the described cases categorized under mutual links and connected components:

**Description of Synthetic Dataset Tables**

 **1.linktarget Table:**
- Contains the mapping of pages to their link targets, representing outgoing links from a source page to a target page. This table is foundational for understanding page relationships.The table contain header as lt_id,lt_title,lt_namespace.

**2.page Table:**
- Includes metadata about individual Wikipedia pages, such as page IDs and titles. It serves as the primary reference for all pages in the dataset. This contains table header as page_id,page_title,page_namespace,page_content_model,page_is_redirect.

**3.redirect Table:**
- Stores information about page redirects, mapping source pages to their redirect targets. This table helps resolve links that point to redirected pages.This table contains rd_from,rd_namespace,rd_title,rd_fragment.

**4.pagelinks Table:**
- Represents the raw links between pages, detailing which pages link to others. This table contains pl_from,pl_from_namespace,pl_target_id.

**Mutual Link Test Case:**

**Simple Mutual Links that help form make connected componets**
  
**Case 1: A ↔ B**

- Page A links to Page B, and Page B links back to Page A.
  
- Represents a straightforward bidirectional relationship.

**Case 2: B ↔ W**

- Page B links to Page W, and Page W links back to Page B.
  
- Another simple bidirectional connection.
  
**Case 3: W ↔ XY**

- Page W links to Page XY, and Page XY links back to Page W.
  
- Forms a basic mutual link.

**Case 4: XY ↔ XZ**

- Page XY links to Page XZ, and Page XZ links back to Page XY.
  
- Demonstrates mutual links with another pair of pages.

- For this Case we will get Conneceted componet as A-B-W-XY-XZ

**Disconnected and Isolated Pages**

**Case 5: D (Isolated)**

- Page D has no outgoing or incoming links.
  
- Represents an isolated node with no connections.

**Redirects in the Graph**

**Case 6: E → F → G**

- Page E links to Page F, which is a redirect to Page G.
  
- Tests handling of a single-level redirection.

**Case 7: E ↔ I via H**

- Page E links to Page H, which redirects to Page I, and Page I links back to Page E.

- Forms a circular connection involving a redirection and gives you a mutual link and connected componet E ↔ H

**Non-Existent Pages**

**Case 8: E → Z**

- Page E links to Page Z, but Page Z does not exist.
  
- Tests the system's handling of invalid or missing pages.

**Chains and Loops**

**Case 9: E → J → K**

- Page E links to Page J, and Page J links to Page K.
  
- Represents a linear chain of connections.

 **Case 10: M ↔ A via N**

- Page M links to Page N, which redirects to Page A, and Page A links back to Page M.
  
- Forms a loop with a redirect and gives you mutual link of M connect with A and form a big connceted componet with A

 **Case 11: O ↔ Q ↔ P with X → Y**

- Page O links to Pages P and Q.
  
- Page Q links back to Pages O and P, and also links to Page X.
  
- Page X redirects to Page Y, and Page Y links back to Page Q.
  
- Represents a complex loop with multi-level redirects and forms a mutual links of O ↔ Q , Q ↔ P and Q↔Y and makes another connceted componet :O-Q-P-Y

**Redirects to Missing Pages**

**Case 12: R → S → T (Non-existent)**

- Page R links to Page S, which redirects to a non-existent Page T.
  
- Tests redirection chains ending in missing pages.

**Complex Cycles**

**Case 13: AB ↔ AP via AH, AI, and AJ**

- Page AB links to Page AH.
  
- Page AH redirects to Page AI, which links to Page AJ.
  
- Page AJ redirects to Page AP, and Page AP links back to Page AB.
  
- Forms a circular connected component with multiple redirects.

**Cases I consider for connected Componets from Mutual link case are :**

**Simple Mutual Links that help form make connected componets**

**Case 1: A ↔ B**

- Page A links to Page B, and Page B links back to Page A.
  
- Represents a straightforward bidirectional relationship.

**Case 2: B ↔ W**

- Page B links to Page W, and Page W links back to Page B.
  
- Another simple bidirectional connection.
  
**Case 3: W ↔ XY**

- Page W links to Page XY, and Page XY links back to Page W.
  
- Forms a basic mutual link.

**Case 4: XY ↔ XZ**

- Page XY links to Page XZ, and Page XZ links back to Page XY.
  
- Demonstrates mutual links with another pair of pages.

- For this Case we will get Conneceted componet as A-B-W-XY-XZ

**Case 7: E ↔ I via H**

- Page E links to Page H, which redirects to Page I, and Page I links back to Page E.

- Forms a circular connection involving a redirection and gives you a mutual link and connected componet E ↔ H

**Case 11: M ↔ A via N**

- Page M links to Page N, which redirects to Page A, and Page A links back to Page M.
  
- Forms a loop with a redirect and gives you mutual link of M connect with A and form a big connceted componet with A that makes M-A-B-W-XY-XZ

**Case 11: O ↔ Q ↔ P with X → Y**

- Page O links to Pages P and Q.
  
- Page Q links back to Pages O and P, and also links to Page X.
  
- Page X redirects to Page Y, and Page Y links back to Page Q.
  
- Represents a complex loop with multi-level redirects and forms a mutual links of O ↔ Q , Q ↔ P and Q↔Y and makes another connceted componet :O-Q-P-Y



**Complex Cycles**

**Case 13: AB ↔ AP via AH, AI, and AJ**

- Page AB links to Page AH.
  
- Page AH redirects to Page AI, which links to Page AJ.
  
- Page AJ redirects to Page AP, and Page AP links back to Page AB.
  
- Forms a circular connected component with multiple redirects.
  

**How To Run Automate Test Suite "unit1test.py"**

- When running this synthetic data in my automatic test suite i was able to write match my expected mutual link with my actual code mutual link output and I was able to load my mutual link locally . And for connected componet I was able to run 4 iteration and was able to write/read from the checkpoint every 3 iteration .

**Code to run the test file** :

- First put everything inside a folder(like main folder test), keep/download  the synthetic data set Page,Pagelink,Redirect and Pagelink from **folder "test-data"** above and keep it  **inside the new folder**  along with the **unit1test code** and the **main mutual link code** .

- Create another test-workspace inside the **main test folder** to write the checkpoint, mutual_links and wikipedia_componets.

- In cmd go inside the directory where you have downloaded the code and run :
  ```
  python -m unittest unit1test.py
  ```

  # **How To Create A Zip File**
  
  For the Zip file of my main mutuallink python file:
  
  - In powershell go inside the directory where you have downloaded the code and run :
    
  ```
  Compress-Archive -Path mutuallink.py -DestinationPath mutuallink.zip
  ```
  
# **Reflection on Project 1 :**

While working on this project, I initially struggled to understand the details of the tables and how to pull data from Parquet. This was my first experience using Spark and running an EMR cluster, which introduced me to the core components of data processing in a distributed environment. Through this project, I learned how Wikipedia’s data is structured and how pages are interconnected, including the challenges posed by redirects and complex links.

One of the main difficulties I encountered was connecting my Spark session to the EMR Hadoop cluster. Once I overcame that, I faced the challenge of processing tables with millions of rows, which was time-consuming. As I continued, I discovered how Spark performs lazy evaluation and learned the importance of persisting data to optimize processing time. Another significant hurdle was running my code in non-interactive EMR steps, where I experienced continuous terminations and step failures, making debugging difficult. Eventually, I learned to set up the necessary roles and permissions and realized that remote sessions were unnecessary, as we weren't connecting remotely. This project was challenging, but it was a great learning experience. I gained valuable insights into the complexity of data cleaning, how EMR steps function, and the intricacies of Wikipedia’s linking structure, with its cyclic links, redirects, and missing pages that mirror the real-world complexities of graph datasets that were interesting.





# **Reflection on Project 2**

What first intrigued me about Project 2 was how it continued from Project 1, connecting the concept of mutual links to graph theory, specifically the identification of connected components, nodes, and vertices. The initial challenge was understanding the core concepts of connected components, including how to represent edges, create bidirectional links, and assign component IDs to each vertex. I assumed that we could use iterative methods to find connected components in large datasets, but my initial implementation took over five hours to complete, and I struggled for days trying to pinpoint the issue. Eventually, I learned the importance of persisting tables and writing checkpoints to S3, allowing the code to resume iterations efficiently. Another important lesson was dividing the code into smaller, manageable sections. This strategy improved the code’s performance and made it easier to debug.This adjustment significantly reduced processing time and helped me achieve a runtime of 28-30 minutes for the entire workflow, including Project 1. Additionally, I discovered how to write an automated test suite and synthetic data to validate the correctness of my code, which was both interesting and challenging since I had never done it before. Debugging environment issues was particularly frustrating, as testing data locally required storing and reading checkpoints from a local directory. It took me days to debug, only to find out that the problem stemmed from Windows-specific limitations. Running the code in a different environment solved the issue, which taught me a lot about compatibility and environment-specific challenges.

Project 2 allowed me to dive deep into handling large graphs with PySpark and analyze the Wikipedia dataset's connected components. One of the biggest difficulties was managing the computational load associated with multi-level redirects, ensuring that they didn’t create unintended loops or break components. What stood out was the distribution of connected component sizes—while a few large clusters represented tightly-knit groups of pages, there were many smaller or isolated nodes, reflecting the diversity and complexity of Wikipedia's structure.

