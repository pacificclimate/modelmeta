# ncWMS configurator

Queries the metadata database and creates an ncWMS config file from the results

## Installation

```bash
virtualenv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt -i https://pypi.pacificclimate.org/simple/
```

## Usage

Built in help is provided: `python ncwms_configurator.py -h`

An example command would be:

```
python ncwms_configurator.py -d postgresql://httpd_meta@monsoon/pcic_meta -o out.xml -e downscaled_canada create
```

And the output xml something like:

```xml
<config>
  <datasets>
    <dataset dataReaderClass="" title="rx1dayETCCDI_aMon_CanESM2_rcp26_r1i1p1_20100101-20391231" id="rx1dayETCCDI_aMon_CanESM2_rcp26_r1i1p1_20100101-20391231" disabled="false" location="/storage/data/climate/CLIMDEX/CMIP5/multidecadal_means/CCCma/CanESM2/rcp26/monClim/atmos/aMon/r1i1p1/v20141101/rx1dayETCCDI/rx1dayETCCDI_aMon_CanESM2_rcp26_r1i1p1_20100101-20391231.nc" copyrightStatement="" updateInterval="-1" moreInfo="" queryable="true">
      <variable scaling="linear" disabled="false" palette="rainbow" numColorBands="250" title="rx1dayETCCDI" colorScaleRange="0.0 50.0" id="rx1dayETCCDI"/>
    </dataset>
    [...]
    [...]
    <dataset dataReaderClass="" title="5var_day_CCSM3_A1B_run1_19500101-20991231" id="5var_day_CCSM3_A1B_run1_19500101-20991231" disabled="false" location="/storage/data/climate/hydrology/vic/gen1/5var_day_CCSM3_A1B_run1_19500101-20991231.nc" copyrightStatement="" updateInterval="-1" moreInfo="" queryable="true">
      <variable scaling="linear" disabled="false" palette="rainbow" numColorBands="250" title="lwe_thickness_of_moisture_content_of_soil_layer" colorScaleRange="0.0111 1.12804" id="sm"/>
      <variable scaling="linear" disabled="false" palette="rainbow" numColorBands="250" title="lwe_thickness_of_water_evaporation_amount" colorScaleRange="-0.0045032 0.0294232" id="aet"/>
      <variable scaling="linear" disabled="false" palette="rainbow" numColorBands="250" title="lwe_thickness_of_surface_runoff_amount" colorScaleRange="0.0 0.618396" id="R"/>
      <variable scaling="linear" disabled="false" palette="rainbow" numColorBands="250" title="lwe_thickness_of_subsurface_runoff_amount" colorScaleRange="0.0 0.0704988" id="bf"/>
      <variable scaling="linear" disabled="false" palette="rainbow" numColorBands="250" title="lwe_thickness_of_surface_snow_amount" colorScaleRange="0.0 231.17" id="swe"/>
    </dataset>
  </datasets>
  <threddsCatalog/>
  <contact>
    <organization></organization>
    <email></email>
    <name></name>
    <telephone></telephone>
  </contact>
  <server>
    <allowglobalcapabilities>true</allowglobalcapabilities>
    <url></url>
    <allowFeatureInfo>True</allowFeatureInfo>
    <title>My ncWMS server</title>
    <keywords></keywords>
    <maxImageWidth>1024</maxImageWidth>
    <adminpassword>ncWMS</adminpassword>
    <maxImageHeight>1024</maxImageHeight>
    <abstract></abstract>
  </server>
  <cache enabled="true">
    <maxNumItemsInMemory>200</maxNumItemsInMemory>
    <enableDiskStore>true</enableDiskStore>
    <elementLifetimeMinutes>1440</elementLifetimeMinutes>
    <maxNumItemsOnDisk>2000</maxNumItemsOnDisk>
  </cache>
  <dynamicServices/>
</config>
```
