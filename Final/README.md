# Final Project Report

# Running the code:

## Files

You will need to have the `centerline.json` and `PIDs.txt` files in the same directory as the pyspark script, `BDM_final_azayev.py`. These will be passed in as files in the command to run.

## Command to run

** Note that this is written for Python 3

```bash
spark-submit --num-executors 6 --executor-cores 5 --executor-memory 10G --files centerline.json,PIDs.txt BDM_final_azayev.py azayev_final_output
```

Note: `azayev_final_output` is where the output goes.

# What has been done

I have found the OLS Coefficient (Slope) (BONUS) for each street segment of the years as well as the total number of reports per street segment. The output has no header, has every single physical ID present (even if there were not matches to it), and is sorted by the physical ID.

# Last Successful Run

The latest successful run has the following statistics:

- Run time: 7m, 11s
- Application ID: application_1594661542558_2805
- User: jazayev000 (my user)
- Output folder in HDFS on my user: test5 (note, multiple different trials of my code have test<test num>, 5th is latest successful). **Note: The only changes I made to the submitted final file is added comments**.

## Total Violations Found

The final number of violations I found was: **29305147**

I did so by running the following script on my output:

```python
import sys

data = []
with open(sys.argv[1], "r") as f:
    for line in f:
        data.append([int(i.strip("\n")) for i in line.split(",")[1:-1]])

S = 0
for item in data:
    S += sum(item)

print(S)

```

Which counts that total number of violations (excluding the OLS coef and physical ID).


# Methodology - A few key points

I have aggressively pre-processed data out of the centerline data. The two most important things I pre-processed:
- PIDs into their own file (used for later merging all non-matched PIDs).
- Re-arranged centerline into a dict with street label and full_stree label, keeping only necessary data.

First, the data from centerline raw has a ton of extra data that we don't need, which we can get rid of. Essentially, we only want the following fields:
- physicalid
- l_row_hn
- h_row_hn
- r_row_hn
- borocode
- steet label
- full street

## centerline

### Optimizing searching for streets / reducing data

The issue with these being in a list is that in order to find a street label or full street match, we would need to for each data point iterate at max 89,000 elements! That is insane, and when I tried to run it just to see what would happen just like that, the cluster would run for over 20 minutes, at which point I would just kill it. Thus, it was necessary to find a better solution.

I did that by unpacking the list, and making a new structure. This structure has 2 fields, `street` and `full street``. Street holds all of the important data, including a list of physical ids, and house ranges information. The full street just holds a list of all matches streets, which in most cases was 1 (but in case, I still kept it a list. Isn't a huge hit on performance). Thus, if street wasn't a hit, I'd also check whether full_street was a hit instead and redirect to check that street's data.

This was a massive upgrade, as now searching didn't take 89,000 searches in the worst case, but around 10 (the longest street segment). It also massively reduced the size of the data.

There were a few things I also did in order to try and keep the size of the list low. For example, when generating the full_street field, the full_street would have many duplicates of the same street. (Worst case being the longest segment). To cut total searching through this list and the total data, I did not append duplicate streets to the full_street data.

### Street Label change for optimization

In order to remove case sensitivity, all non-numerical letters are capitalized. Additionally, because there was inconsistencies in spacing in the data, I stripped all white spaces as well. This could possible have caused a problem if I were to compare using a membership operation, but I matched exactly thus this sort of mismatch shouldn't have happened.

## PIDs

This is just the PIDs extracted and sorted as a list. I then do a full outer join on this in order to make sure all PIDs are in the final dataset.


## Matching the streets

There were a few things with the data that prevented matchings:
- Multiple spaces in the centerline data (view the centerline fragment to see solution for that in the dataset)
- Inconsistencies in the names between datasets. For example, `7TH AVE` in the violations dataset, but `7 AVE` in the centerline dataset. The largest categories included `TH`, `ST`, and `ND` endings.
- Addresses with numbers at the end. Example: `35A`. The `A` in this case could be completely disregarded, as that can be seen as a 'sub address' or house number' rather than a 'house address'.
- Many names for the same street. For example, violations dataset looked for `BEACH AVE` whereas in the centerline it was either `BAVE` or `BCHAVE`.
- Human data entry error - For example, when given a home address `91 29`, it should have been `92-29`. This is an extremely broad category and was discarded, as there are too many possible error types that might not happen often enough to really care about.




