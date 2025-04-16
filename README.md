# Data Engineering

This repository contains implementations for data engineering assignments focusing on big data management, processing, and analytics.

## Assignment 1: Parallel Database Processing

Implementation of parallel database algorithms for handling large datasets efficiently:

- **Parallel Searching**: Implementation of a parallel search algorithm with linear search capability that works with hash partitioning
- **Parallel Joining**: Implementation of parallel join algorithms including hash join and sort-merge join 
- **Parallel Sorting**: Implementation of a parallel sorting algorithm 
- **Parallel Grouping**: Implementation of a parallel grouping algorithm

### Key Techniques

```python
# Example of my implementation of linear search
def linear_search(data, key):
    matches = []
    for record in data:
        if record[1] == key:
            matches.append(record)
    return matches
```

## Assignment 2: StopFire - Fire Prediction System

A comprehensive system for fire prediction and analysis using big data technologies developed for the StopFire campaign by Monash University. This system ingests, processes, and analyzes large amounts of sensor data from various Victorian cities to predict and prevent fires.

### Project Overview

StopFire has deployed sensors across Victoria to collect climate and fire data. Due to the large volume of data, their existing techniques were unable to provide timely predictions. This implementation creates a complete data pipeline using NoSQL database (MongoDB) for storage, Apache Kafka for streaming, and Apache Spark Streaming for real-time processing.

### Datasets

The system works with five core datasets:
- `hotspot_historic.csv`: Historical fire hotspot data
- `climate_historic.csv`: Historical climate data
- `hotspot_AQUA_streaming.csv`: Streaming data from NASA's AQUA satellite
- `hotspot_TERRA_streaming.csv`: Streaming data from NASA's TERRA satellite
- `climate_streaming.csv`: Streaming climate sensor data

### Part A: Data Model and Static Data Processing

#### Task 1: MongoDB Data Model

I designed an embedded data model where each document represents one day of climate data with any associated hotspot events embedded as an array within the climate document.

```json
{
  "_id": ObjectId("663e3650c366387f1b054c70"),
  "date": ISODate("2023-12-13T00:00:00.000Z"),
  "station": "948702",
  "climate": {
    "air_temperature_celcius": 28,
    "relative_humidity": 49.9,
    "windspeed_knots": 11,
    "max_wind_speed": 19,
    "precipitation": "0.00I",
    "ghi_wm2": 240
  },
  "hotspots": [
    {
      "latitude": -36.6296,
      "longitude": 142.5191,
      "datetime": ISODate("2023-12-13T00:00:00.000Z"),
      "confidence": 70,
      "surface_temperature_celcius": 45,
      "cause": "natural"
    }
  ]
}
```

**Justification for the Embedded Model:**

1. **Natural Relationship Mapping**: There is a clear one-to-many relationship between climate and hotspot data where each climate record for a day can have zero, one, or many associated hotspot records.

2. **Query Efficiency**: Most queries will need to retrieve climate data for a date along with any related hotspots. The embedded model allows retrieving all this data with a single query without needing expensive joins.

3. **Atomic Updates**: When new hotspot data arrives, it can be pushed into the hotspots array as part of a single atomic update operation.

4. **Simplified Streaming Processing**: As new data arrives from the streams, hotspot events can easily be appended to the corresponding climate document.

5. **Fire Cause Determination**: The embedded model makes it easy to determine the cause of fires since the relevant climate conditions are in the same document.

6. **Manageable Document Size**: While embedding introduces some data redundancy, the number of hotspots per day is likely small and bounded, keeping document sizes below MongoDB's 16MB limit.

#### Task 2: Querying MongoDB using PyMongo

I implemented a data loading process that reads the CSV files and loads them into MongoDB following the embedded model:

```python
# Read hotspot data and build a dictionary grouping hotspots by date
hotspot_dict = {}
for _, row in hotspot_data.iterrows():
    date = date_to_datetime(datetime.strptime(row["date"], "%d/%m/%Y").date())
    if date not in hotspot_dict:
        hotspot_dict[date] = []
    
    hotspot_dict[date].append({
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "datetime": datetime.strptime(row["datetime"], "%d/%m/%Y %H:%M:%S"),
        "confidence": row["confidence"],
        "surface_temperature_celcius": row["surface_temperature_celcius"]
    })

# Read climate data and create documents with embedded hotspots
for _, row in climate_data.iterrows():
    date = date_to_datetime(datetime.strptime(row["date"], "%d/%m/%Y").date())
    
    # Create climate document
    climate_doc = {
        "date": date,
        "station": row["station"],
        "climate": {
            "air_temperature_celcius": row["air_temperature_celcius"],
            "relative_humidity": row["relative_humidity"],
            "windspeed_knots": row["windspeed_knots"],
            "max_wind_speed": row["max_wind_speed"],
            "precipitation": row["precipitation"],
            "ghi_wm2": row["ghi_wm2"]
        },
        "hotspots": hotspot_dict.get(date, [])
    }
    
    # Insert into MongoDB
    collection.insert_one(climate_doc)
```

I then implemented ten different MongoDB queries using PyMongo:

1. **Find climate data for a specific date**:
```python
result = collection.find_one({"date": datetime(2023, 12, 12)}, 
                            {"climate": 1, "date": 1, "_id": 0})
```

2. **Find hotspots with surface temperature in a specific range**:
```python
pipeline = [
    {"$unwind": "$hotspots"},
    {"$match": {
        "hotspots.surface_temperature_celcius": {"$gte": 65, "$lte": 100}
    }},
    {"$project": {
        "latitude": "$hotspots.latitude",
        "longitude": "$hotspots.longitude",
        "surface_temperature_celcius": "$hotspots.surface_temperature_celcius",
        "confidence": "$hotspots.confidence",
        "_id": 0
    }}
]
results = collection.aggregate(pipeline)
```

3. **Find climate and hotspot data for specific dates**:
```python
pipeline = [
    {"$match": {
        "date": {"$in": [datetime(2023, 12, 15), datetime(2023, 12, 16)]}
    }},
    {"$project": {
        "date": 1,
        "surface_temperature_celcius": {"$ifNull": [{"$avg": "$hotspots.surface_temperature_celcius"}, "N/A"]},
        "air_temperature_celcius": "$climate.air_temperature_celcius",
        "relative_humidity": "$climate.relative_humidity",
        "max_wind_speed": "$climate.max_wind_speed",
        "_id": 0
    }}
]
results = collection.aggregate(pipeline)
```

4. **Find high confidence hotspots**:
```python
pipeline = [
    {"$unwind": "$hotspots"},
    {"$match": {
        "hotspots.confidence": {"$gte": 80, "$lte": 100}
    }},
    {"$project": {
        "datetime": "$hotspots.datetime",
        "air_temperature_celcius": "$climate.air_temperature_celcius",
        "surface_temperature_celcius": "$hotspots.surface_temperature_celcius",
        "confidence": "$hotspots.confidence",
        "_id": 0
    }}
]
results = collection.aggregate(pipeline)
```

5. **Find hotspots with highest surface temperature**:
```python
pipeline = [
    {"$unwind": "$hotspots"},
    {"$sort": {"hotspots.surface_temperature_celcius": -1}},
    {"$limit": 10},
    {"$project": {
        "date": 1,
        "latitude": "$hotspots.latitude",
        "longitude": "$hotspots.longitude",
        "surface_temperature_celcius": "$hotspots.surface_temperature_celcius",
        "confidence": "$hotspots.confidence",
        "_id": 0
    }}
]
results = collection.aggregate(pipeline)
```

6. **Count fires each day**:
```python
pipeline = [
    {"$project": {
        "date": 1,
        "total_fires": {"$size": {"$ifNull": ["$hotspots", []]}},
        "_id": 0
    }}
]
results = collection.aggregate(pipeline)
```

7. **Find low confidence fires**:
```python
pipeline = [
    {"$unwind": "$hotspots"},
    {"$match": {
        "hotspots.confidence": {"$lt": 70}
    }},
    {"$project": {
        "date": 1,
        "hotspots": 1,
        "_id": 0
    }}
]
results = collection.aggregate(pipeline)
```

8. **Calculate average surface temperature by day**:
```python
pipeline = [
    {"$project": {
        "date": 1,
        "avg_surface_temperature": {
            "$cond": [
                {"$eq": [{"$size": {"$ifNull": ["$hotspots", []]}}, 0]},
                "N/A",
                {"$avg": "$hotspots.surface_temperature_celcius"}
            ]
        },
        "_id": 0
    }}
]
results = collection.aggregate(pipeline)
```

9. **Find climate records with lowest GHI**:
```python
results = collection.find({}, {
    "date": 1, 
    "climate.ghi_wm2": 1, 
    "_id": 0
}).sort("climate.ghi_wm2", 1).limit(10)
```

10. **Find specific precipitation records**:
```python
results = collection.find({
    "climate.precipitation": {"$regex": "^0\\.2[0-9]|^0\\.3[0-5]"}
}, {
    "date": 1,
    "climate.precipitation": 1,
    "_id": 0
})
```

I also created indexes to optimize query performance:

```python
# Create indexes for common query patterns
collection.create_index("date")  # For queries filtering by date
collection.create_index("hotspots.surface_temperature_celcius")  # For range queries on temperature
collection.create_index("hotspots.confidence")  # For filtering on confidence levels
collection.create_index("climate.ghi_wm2")  # For sorting by GHI
```

**Index Justification**:
1. **Date Index**: Most queries filter by date, making this a critical index for performance
2. **Surface Temperature Index**: Used for range queries and sorting by temperature
3. **Confidence Index**: Used for filtering hotspots by confidence levels
4. **GHI Index**: Used for sorting and filtering by Global Horizontal Irradiance

These indexes balance query performance against write performance, as the system needs to handle high data ingestion rates from the streams.

### Part B: Real-time Streaming and Processing

#### Task 1: Processing Data Stream

I implemented a complete data streaming pipeline with three Kafka producers and a Spark Streaming application.

##### Event Producer 1: Climate Data

```python
# Climate Producer publishing weather data every 10 seconds
def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(
            bootstrap_servers=[f"{hostip}:9092"], api_version=(0, 10)
        )
    except Exception as ex:
        print("Exception while connecting Kafka.")
        print(str(ex))
    finally:
        return _producer

# Latest date from climate collection
latest_date = datetime(2024, 1, 1)  # Starting date

# Load climate data
data = pd.read_csv("climate_streaming.csv")

# Feeding data loop
while True:
    # Randomly select a row with replacement
    row = data.sample(n=1).to_dict(orient="records")[0]
    
    # Add producer ID and date
    row["producer_id"] = "climate_producer"
    row["created_date"] = latest_date.strftime("%Y-%m-%d")
    
    # Publish to Kafka
    publish_message(producer, topic, "parsed", dumps(row))
    
    # Update date (10 seconds = 1 day)
    latest_date += timedelta(days=1)
    
    # Wait 10 seconds
    sleep(10)
```

##### Event Producer 2: AQUA Satellite Data

```python
# AQUA Producer with random timing
data = pd.read_csv("hotspot_AQUA_streaming.csv")

# Feeding data loop
while True:
    # Randomly select a row
    row = data.sample(n=1).to_dict(orient="records")[0]
    
    # Add producer ID
    row["producer_id"] = "aqua_producer"
    
    # Generate random time
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    row["datetime"] = f"{hour:02d}:{minute:02d}:{second:02d}"
    
    # Publish to Kafka
    publish_message(producer, topic, "parsed", dumps(row))
    
    # Random delay (3-7 seconds)
    sleep(random.randint(3, 7))
```

##### Event Producer 3: TERRA Satellite Data

```python
# TERRA Producer with random timing
data = pd.read_csv("hotspot_TERRA_streaming.csv")

# Feeding data loop
while True:
    # Randomly select a row
    row = data.sample(n=1).to_dict(orient="records")[0]
    
    # Add producer ID
    row["producer_id"] = "terra_producer"
    
    # Generate random time
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    row["datetime"] = f"{hour:02d}:{minute:02d}:{second:02d}"
    
    # Publish to Kafka
    publish_message(producer, topic, "parsed", dumps(row))
    
    # Random delay (4-8 seconds)
    sleep(random.randint(4, 8))
```

##### Streaming Application

The streaming application processes data in batches and handles several complexities:

1. **Geohashing for Location Matching**:
```python
def geohash_encode(latitude, longitude, precision=3):
    return geohash2.encode(latitude, longitude, precision)

def is_within_proximity(geohash1, geohash2, precision):
    return geohash1[:precision] == geohash2[:precision]
```

2. **Merging Duplicate Hotspot Reports**:
```python
def sync_fix(hotspots):
    merged_hotspots = []
    
    for i in range(len(hotspots)):
        if hotspots[i] is None:
            continue
            
        merged_hotspot = hotspots[i].copy()
        count = 1
        merging_hotspots = [hotspots[i]]
        
        for j in range(i + 1, len(hotspots)):
            if hotspots[j] is None:
                continue
                
            # Check if hotspots are within proximity (geohash precision 5)
            # and time difference is <= 10 minutes
            if is_within_proximity(
                hotspots[i]["geohash5"], hotspots[j]["geohash5"], 4
            ) and abs(
                datetime.strptime(hotspots[i]["datetime"], "%H:%M:%S")
                - datetime.strptime(hotspots[j]["datetime"], "%H:%M:%S")
            ) <= timedelta(minutes=10):
                # Merge hotspots
                merged_hotspot["confidence"] += hotspots[j]["confidence"]
                merged_hotspot["surface_temperature_celcius"] += hotspots[j]["surface_temperature_celcius"]
                count += 1
                merging_hotspots.append(hotspots[j])
                hotspots[j] = None
                
        # Calculate averages if merging occurred
        if count > 1:
            merged_hotspot["confidence"] = round(merged_hotspot["confidence"] / count)
            merged_hotspot["surface_temperature_celcius"] = round(
                merged_hotspot["surface_temperature_celcius"] / count
            )
            merged_hotspots.append(merged_hotspot)
        else:
            merged_hotspots.append(hotspots[i])
            
    return merged_hotspots
```

3. **Fire Cause Determination**:
```python
def determine_fire_cause(air_temperature, ghi):
    return "natural" if air_temperature > 20 and ghi > 180 else "other"
```

4. **Batch Processing Logic**:
```python
def process_batch(batch_data):
    # Extract climate and hotspot data
    climate_records = []
    hotspot_records = []
    
    for record in batch_data:
        if record["producer_id"] == "climate_producer":
            climate_records.append(record)
        else:  # AQUA or TERRA
            hotspot_records.append(record)
    
    # Handle multiple or no climate records
    if not climate_records:
        print("No climate records in batch. Skipping...")
        return
        
    # Select one climate record (using the latest)
    climate_record = sorted(climate_records, key=lambda x: x["created_date"])[-1]
    climate_data = process_climate_data(climate_record)
    
    # Process hotspots
    matching_hotspots = []
    
    for hotspot in hotspot_records:
        hotspot_data = process_hotspot_data(hotspot)
        
        # Check if hotspot is within proximity of climate location
        if is_within_proximity(
            hotspot_data["geohash3"],
            climate_data["geohash3"],
            3
        ):
            matching_hotspots.append(hotspot_data)
    
    # Merge duplicate hotspot reports
    if matching_hotspots:
        merged_hotspots = sync_fix(matching_hotspots)
        
        # Determine fire cause for each hotspot
        for hotspot in merged_hotspots:
            hotspot["cause"] = determine_fire_cause(
                climate_data["climate"]["air_temperature_celcius"],
                climate_data["climate"]["ghi_wm2"]
            )
        
        # Add hotspots to climate document
        climate_data["hotspots"] = merged_hotspots
    
    # Store in MongoDB
    collection.insert_one(climate_data)
```

This implementation handles several edge cases:
- If no climate record is in a batch, processing is skipped
- If multiple climate records appear in a batch, only one is selected
- Hotspots are only processed if they match a climate location
- Duplicate hotspot reports are merged based on location and time proximity
- Fire causes are determined based on climate conditions

#### Task 2: Data Visualization

I implemented both real-time and static visualizations for the data.

##### Streaming Data Visualization

```python
def consume_messages(consumer, fig, ax):
    x, y = [], []
    
    for message in consumer:
        data = message.value
        
        # Process climate data
        message_date = datetime.fromisoformat(data["created_date"])
        arrival_time = datetime.now().strftime("%d/%m/%y %H:%M:%S")
        air_temp = data["air_temperature_celcius"]
        
        # Update data
        x.append(arrival_time)
        y.append(air_temp)
        
        # Update plot
        ax.clear()
        ax.plot(x, y, 'r-')
        ax.set_xlabel("Arrival Time")
        ax.set_ylabel("Air Temperature (Celsius)")
        
        # Annotate max and min points
        if len(y) > 1:
            annotate_max(x, y, ax)
            annotate_min(x, y, ax)
        
        # Format x-axis
        if len(x) > 20:
            ax.set_xticks(x[::5])
            ax.set_xticklabels(x[::5], rotation=45)
        
        fig.canvas.draw()
        fig.canvas.flush_events()
```

This visualization shows air temperature changes over time and highlights maximum and minimum values.

##### Static Data Visualization

1. **Bar Chart of Fire Records by Hour**:

```python
def visualize_fire_by_hour():
    # Connect to MongoDB
    client = MongoClient(hostip, 27017)
    db = client["fit3182_assignment_db"]
    collection = db["climate_stream"]
    
    # Aggregate fire counts by hour
    pipeline = [
        {"$unwind": "$hotspots"},
        {"$project": {
            "hour": {"$hour": "$hotspots.datetime"},
            "_id": 0
        }},
        {"$group": {
            "_id": "$hour",
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    
    results = list(collection.aggregate(pipeline))
    
    # Create bar chart
    hours = [r["_id"] for r in results]
    counts = [r["count"] for r in results]
    
    plt.figure(figsize=(12, 6))
    bars = plt.bar(hours, counts, color='orange')
    
    # Add labels and title
    plt.xlabel('Hour of Day')
    plt.ylabel('Number of Fire Records')
    plt.title('Fire Records by Hour')
    plt.xticks(range(0, 24))
    
    # Add count values above bars
    for bar, count in zip(bars, counts):
        plt.text(
            bar.get_x() + bar.get_width()/2,
            bar.get_height() + 0.5,
            str(count),
            ha='center',
            va='bottom'
        )
    
    plt.tight_layout()
    plt.show()
```

2. **Interactive Map Visualization**:

```python
def create_fire_map():
    # Connect to MongoDB
    client = MongoClient(hostip, 27017)
    db = client["fit3182_assignment_db"]
    collection = db["climate_stream"]
    
    # Get all fire data with relevant fields
    pipeline = [
        {"$unwind": "$hotspots"},
        {"$project": {
            "latitude": "$hotspots.latitude",
            "longitude": "$hotspots.longitude",
            "cause": "$hotspots.cause",
            "surface_temp": "$hotspots.surface_temperature_celcius",
            "air_temp": "$climate.air_temperature_celcius",
            "relative_humidity": "$climate.relative_humidity",
            "confidence": "$hotspots.confidence",
            "_id": 0
        }}
    ]
    
    fire_data = list(collection.aggregate(pipeline))
    
    # Create map centered on Victoria, Australia
    m = folium.Map(
        location=[-37.0, 145.0],
        zoom_start=7,
        tiles='CartoDB positron'
    )
    
    # Add markers for each fire
    for fire in fire_data:
        # Determine marker color based on cause
        color = 'blue' if fire['cause'] == 'natural' else 'red'
        
        # Create tooltip content
        tooltip = f"""
        <strong>Fire Details</strong><br>
        Air Temperature: {fire['air_temp']}°C<br>
        Surface Temperature: {fire['surface_temp']}°C<br>
        Relative Humidity: {fire['relative_humidity']}%<br>
        Confidence: {fire['confidence']}%<br>
        Cause: {fire['cause']}
        """
        
        # Add marker to map
        folium.Marker(
            location=[fire['latitude'], fire['longitude']],
            tooltip=folium.Tooltip(tooltip),
            icon=folium.Icon(color=color)
        ).add_to(m)
    
    # Save map to HTML file
    m.save('fire_map.html')
    
    return m
```

This creates an interactive map showing fire locations with blue markers for natural fires and red for others. Tooltips provide detailed information about each fire.

### Part C: Interview and Demo

For the interview and demo, I prepared a comprehensive presentation that showcased:

1. **System Architecture Overview**:
   - Explanation of the data model design and justification
   - Walkthrough of the streaming pipeline components

2. **Live Demonstration**:
   - Running all three producers simultaneously
   - Showing the streaming application processing data in real-time
   - Displaying visualizations updating with new data

3. **Technical Deep Dive**:
   - Explanation of geohashing implementation for location matching
   - Demonstration of the duplicate hotspot merging algorithm
   - Discussion of MongoDB query optimization through indexing

4. **Performance Considerations**:
   - Handling of high data ingestion rates
   - Optimization of batch processing
   - Management of MongoDB write operations

### Setup and Running Instructions

To run the complete system:

1. **Start MongoDB**:
   ```
   mongod --bind_ip 192.168.10.125
   ```

2. **Start Kafka and Zookeeper**:
   ```
   bin/zookeeper-server-start.sh config/zookeeper.properties
   bin/kafka-server-start.sh config/server.properties
   ```

3. **Create Kafka Topics**:
   ```
   bin/kafka-topics.sh --create --topic climate --bootstrap-server 192.168.10.125:9092
   bin/kafka-topics.sh --create --topic hotspot --bootstrap-server 192.168.10.125:9092
   ```

4. **Run the Producers**:
   - Start `Assignment_PartB_Producer1.ipynb` for climate data
   - Start `Assignment_PartB_Producer2.ipynb` for AQUA satellite data
   - Start `Assignment_PartB_Producer3.ipynb` for TERRA satellite data

5. **Start the Streaming Application**:
   - Run `Assignment_PartB_Streaming_Application.ipynb`

6. **View Visualizations**:
   - Run `Assignment_PartB_Data_Visualisation.ipynb` to see real-time plots
   - Explore the generated fire map HTML

### Repository Structure

- **a1/** - Assignment 1 files
  - `parallel_search_join_sort_groupby_algorithms.ipynb`: Implementation of parallel database algorithms
  
- **a2/** - Assignment 2 files
  - `data/`: Contains all datasets
    - `hotspot_historic.csv`: Historical fire hotspot data
    - `climate_historic.csv`: Historical climate data
    - `hotspot_AQUA_streaming.csv`: Streaming data from NASA's AQUA satellite
    - `hotspot_TERRA_streaming.csv`: Streaming data from NASA's TERRA satellite
    - `climate_streaming.csv`: Streaming climate sensor data
  - `Assignment_PartA.ipynb`: MongoDB data model and query implementation
  - `Assignment_PartB_Producer1.ipynb`: Kafka producer for climate data
  - `Assignment_PartB_Producer2.ipynb`: Kafka producer for AQUA satellite data
  - `Assignment_PartB_Producer3.ipynb`: Kafka producer for TERRA satellite data
  - `Assignment_PartB_Streaming_Application.ipynb`: Spark Streaming application
  - `Assignment_PartB_Data_Visualisation.ipynb`: Data visualization implementations