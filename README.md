## Earning At Risk Analysis for NextBusinessEnergy

Empower Analytics has been engaged by Next Business Energy to derive the Earnings-at-Risk (EAR) for the Portfolio 
for a rolling 12-month period.

This repository contains the implementation of all the analysis and AWS resources. 

#### Earnings-At-Risk

The EAR is measured by building simulations of possible outcomes based on the physical characteristics of the NEM, 
and then measure the potential financial impact from an extreme scenario to the expected case (median).

To complete the EAR, the following steps will be undertaken:

1. Build daily and intraday weather simulations across the NEM which will include weather scenarios that may not been 
experienced in recent history
2. Build weather dependent NBE demand simulations which will lead to correlated demand and spot simulations, and the 
demand may be more than experienced in recent history
3. Build weather dependent generation and then spot prices based on the NEM Spot Simulation Forecast model
4. Derive the settlement of all trading positions held by NBE  
5. Derive the spot market cost
6. Combine the spot market cost with the settlement of the hedging positions for each simulation at half-hour 
resolution to derive the cash impact
7. Derive the EAR for each of the following resolutions where:

  * Resolutions are:
    * Weekly
    * Monthly
    * Quarterly
    * Annual  
  * EAR is derived as:
    * EAR-100 is the difference between the 100th percentile and median (50th percentile)
    * EAR-99 is the difference between the 99th percentile and median (50th percentile)
    * EAR-98 is the difference between the 98th percentile and median (50th percentile)
    * EAR-95 is the difference between the 95th percentile and median (50th percentile)

#### Implementation

An automated pipeline using Lambda functions and S3 events is implemented to achieve the regular updates of the 
results at low cost.