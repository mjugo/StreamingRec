# StreamingRec 
An evaluation framework that simulates a real-life recommendation scenario, in which recommendation 
algorithms receive user clicks and newly published articles chronologically. After each 
click, algorithms generate recommendation lists, which are then evaluated by comparing 
them with the next user clicks. Most of the algorithms are implemented so that they learn 
incrementally from every click.

## Implemented recommendation algorithms
*  Most Popular
*  Recently published
*  Recently Popular 
*  Item-Item CF
*  Co-Occurrence
*  Sequential Patterns
*  KNN
*  Lucene
*  BPR
*  Keywords

## Running StreamingRec
### Run on the command line 

1. Download the pre-compiled jar from the [releases tab](https://github.com/mjugo/StreamingRec/releases/latest)
2. Acquire item and click CSV input files (see below)
3. Create algorithm and metric JSON config files (see below)
4. Run with `java -jar StreamingRec.jar <parameters>`
    * Commonly, at least the following parameters should be used: 
    `java -jar StreamingRec.jar --items=<path_to_item_meta_data_file> --clicks=<path_to_click_data_file> --algorithm-config=<path_to_algorithm_json_config_file> --metrics-config=<path_to_metrics_json_config_file> --session-inactivity-threshold`
    * For a full list and description of the available parameters, run with `-h`
    * For systems with small RAM, adjusting the `--thread-count=<N>` parameter can help. By default, it is set to the number of available CPU cores - 1, but in general, less concurrent threads result in less RAM usage.  

### How to acquire input files (data sets)

#### Outbrain
The Outbrain data set is publicly available. To use it with StreamingRec, 
the following steps are necessary. 
 
1. Register an account at Kaggle and download the data set files from 
https://www.kaggle.com/c/outbrain-click-prediction/data. 
Only the following files are necessary:
    * page_views.csv.zip
    * documents_meta.csv.zip
    * documents_categories.csv.zip
    * documents_entities.csv.zip 
    * documents_topics.csv.zip
2. Put all above-mentioned files in one folder.
3. Run the StreamingRec import script with at least the following parameters:
`java -cp StreamingRec.jar tudo.streamingrec.data.loading.ReadOutbrain --input-folder=<folder_to_outbrain_files> --out-items=<path_to_item_output_file> --out-clicks=<path_to_clicks_output_file> --publisher=<id_of_publisher_to_be_extracted>`
4. After processing, two data set files (one for the item meta data and one for the click data) 
will be created that can be used in with StreamingRec.

#### Plista / CLEF-NewsREEL Challenge
The 2016 Plista challenge data set is not publicly available anymore. However, for researchers who still have access to it, the following step need to be executed to use this data set.

1. Collect all the `.gz` files of the data set in one folder
2. Run the first stage import script with 
`java -cp StreamingRec.jar tudo.streamingrec.data.loading.ReadPlista <parameters>`. 
For help about the parameters run with `-h`
3. This generates, intermediate input files. To create the final input files, 
run the second stage import script with 
`java -cp StreamingRec.jar tudo.streamingrec.data.loading.JoinPlistaTransactionsWithMetaInfo <parameters>`. 
For help about the parameters run with `-h`
4. After processing, two data set files (one for the item meta data and one for the click data) 
will be created that can be used in with StreamingRec. The intermediate input files can be deleted.

### How to configure algorithms and metrics
Algorithms and metrics are configured via JSON files, one for algorithms, one for metrics. 
Each of the files contains a JSON array that is made up of one JSON object per algorithm/metric. 
A general metrics configuration can be found in the project folder at `config/metrics-config.json`.
The algorithm configurations used in the _Information Procession & Management_ paper (currently under review) can be found at`config/algorithm-config-ipm-418.json` (for Plista publisher ksta.de) and 
`config/algorithm-config-ipm-43.json` (for Outbrain publisher 43). 
Note that these algorithm configurations can take a long time to run. 
For a quick test, a simple algorithm configuration is included at `config/algorithm-config-simple.json`.

To configure algorithms or metrics manually, a new JSON config file can be created. 
The format of the JSON object representing the algorithm or metric is always the same:

```json
{
    "name": "Co-Occurrence", 
    "algorithm": ".FastSessionCoOccurrence", 
    "wholeSession": false 
}
```
* name: Is the pretty-print name of the algorithm that will appear as an identifier in the result list
* algorithm: the qualified name of the algorithm class relative to the tudo.streamingrec.algorithms package
* wholeSession: Is an additional parameter specific to this algorithm (Co-Occurrence)
    * "Additional parameters" are all parameters that the respective algorithm class offers via its setter methods. 
To find out which parameters are offered by which algorithm, consult the javadoc from the [releases tab](https://github.com/mjugo/StreamingRec/releases/latest)


### Import and run in Eclipse 

1. Download/Clone the source code
2. Import the project into Eclipse via `Import -> Existing Maven Projects`
3. Follow the above-mentioned instructions on how to acquire input files and configure the algorithms and metrics
4. To set parameters, adjust the class attributes directly in `tudo.Framework.StreamingRec` or set the call parameters 
via Eclipse's `Run Configurations` menu
5. Run the class `tudo.Framework.StreamingRec` via `Run as ... -> Java Application`

## Implementing new algorithms / metrics
Custom algorithms have to extend the class `tudo.streamingrec.algorithms.Algorithm`. 
The implementation shall update its model every time the method `void trainInternal(List<Item> items, List<ClickData> transactions)` 
is called and it shall generate a recommendation list every time the method 
`LongArrayList recommend(ClickData clickData)` is called.

Custom metrics have to extend the class `tudo.streamingrec.evaluation.metrics.Metric`. The implementation
shall calculate, update, and store its current metric value every time the method 
`void evaluate(Transaction transaction, LongArrayList recommendations, LongOpenHashSet userTransactions)`
is called. The final metric value shall be returned when the method `double getResults()` is called. 
In any case the value of the class attribute `k` shall be observed.

## Authors
**Author names are redacted because a paper based on a more advanced version of this source code is currently under __blind__ review.**

## License
Copyright \[2017\] \[**Author names are redacted because a paper based on a more advanced version of this source code is currently under __blind__ review.**]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
