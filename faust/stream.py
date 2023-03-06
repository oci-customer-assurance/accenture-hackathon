import faust

app = faust.App('oci-starlink', broker='kafka://localhost')

# Models describe how messages are serialized:
# {"file_name": "JPFS...", data_type": .tiff, etc}
class SatelliteData(faust.Record):
    file_name: str
    data_type: str

@app.agent(value_type=SatelliteData)
async def spaceAssurance(spaceAssurances):
    async for spaceAssurance in spaceAssurances:
        # process infinite stream of SatelliteDatas.
        print(f'SatelliteData for {spaceAssurance.file_name}: {spaceAssurance.data_type}')