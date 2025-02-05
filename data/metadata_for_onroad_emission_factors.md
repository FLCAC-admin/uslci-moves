# METADATA FOR ONROAD EMISSION FACTORS  
Author: Andrew Eilbert (ERG)
Created: 11/5/2024

# MODEL VERSION/SOURCE 
MOVES4.0.1 (Released January 2024)
US Environmental Protection Agency, Office of Transportation and Air Quality

# MOVES DESCRIPTION
The Motor Vehicle Emission Simulator (MOVES) is EPA's regulatory model to determine emission inventories for highway vehicles, including cars, trucks, and buses.
Unlike its predecessor model (known as MOBILE), MOVES combines in-use emission rates and detailed fleet populations and activity--frequently in the form of vehicle miles or kilometers traveled--to calculate inventories.
This is the so called three-legged stool for developing onroad inventories.
In addition, the model can generate inventories for a wide array of different criteria pollutants, greenhouse gases, and air toxics, predominantly for tailpipe exhaust but also namely for particulate matter from brake and tire wear.

While there have been many changes since the first public release of MOVES in 2010, the model has always featured different temporal ranges from hourly to annual analysis and various geographic bounds including link-level (project), county, state, and national domains.
This enables customization with user-supplied inputs, although default data is available for entirely every model parameter, such as vehicle ages, average speeds, and meteorology (that is, temperature and relative humidity).
For these reasons it is the preferred tool for setting federal emission standards, developing EPA's National Emissions Inventory, and demonstrating transportation conformity through state implementation plans and local hot-spot analyses.
While these are the most common use cases, MOVES is also employed for research and non-regulatory purposes by academia, government, and private industry.

# RUN SPECIFICATIONS
One such research application is the Federal LCA Commons, which compiles data from a variety of transportation sources and modes including highway, offroad, rail, aviation, and marine.
The MOVES emission factors for the LCA Commons were developed through annual state-level model runs using 2024 default data for nearly all the pollutants available in MOVES.
The most important model output parameters for LCA Commons emission factors included fuel type and vehicle class called source use type as well as model year (that is necessary to compute vehicle age, although it was not used in the current emission factor calculations).
Also, the LCA Commons does not consider applications outside of freight transportation, so only certain MOVES source types were selected for this analysis: light (heavy-duty) commercial trucks, short- and long-haul single unit trucks (often known as vocational vehicles), and short- and long-haul combination trucks (or tractor-trailers).

Despite that a majority of emissions are produced during vehicle operation, not all emissions occur while the vehicle is running.
These MOVES runs for the LCA Commons include other emission processes, such as idling and truck hotelling (both extended engine idling and auxiliary power unit use) in long-haul sleeper cabs in particular.
However, due to runtime constraints, these model runs do not include evaporative emissions while the vehicle is soaking (engine off) but may be added at a later point.

# EMISSION FACTOR CALCULATIONS
Some further manipulation of MOVES output was performed to produce emission factors (or simply EFs) for the LCA Commons.
First, as mentioned earlier, MOVES most easily reports results as emission inventories, so the inventories were divided by the accompanying activity in vehicle kilometers traveled and average payload to calculate onroad EFs.
The average payloads by MOVES source type were aggregated from a few federal sources, namely the 2002 Vehicle Inventory and Use Survey conducted by the US Census Bureau and 2012 and 2017 Freight Analysis Framework (FAF) statistics collected by the Federal Highway Administration (Source: Research, Development, and Application of Methods to Update Freight Analysis Framework Out-of-Scope Commodity Flow Data and Truck Payload Factors, Chpt. 12, Table 42).

We have taken the average of the 2002, 2012, and 2017 payloads for single unit and combination trucks respectively and then applied a distance weighting from MOVES national defaults to split the short-haul (freight vehicles averaging trips of less than 200 miles per day) and long-haul (averaging more than 200 miles daily) distribution.
The following distance-weighted payloads in US tons were applied: short-haul single unit = 6.50, long-haul single unit = 0.44, all single unit = 6.94, short-haul combination = 6.23, long-haul combination = 9.95, all combination = 16.18, all single unit and combination = 23.12.
Without better data available, the distance-weighted light commercial truck payload was assumed to be 1 US ton.

This resulted in MOVES onroad EFs for the eight source type distinctions for gasoline, diesel, E85, compressed natural gas (CNG), and electricity (though not all these source type-fuel type combinations are populated in MOVES) across 45 pollutants and energy consumption for all 50 states along with the District of Columbia, Puerto Rico, and the Virgin Islands (59,042 rows).
The MOVES EFs are in terms of kilograms per ton-kilometer (kg/ton-km) and energy consumption is in millions of British Thermal Units per ton-kilometer (mmBTU/ton-km) and will be grouped in regions for the LCA Commons.
