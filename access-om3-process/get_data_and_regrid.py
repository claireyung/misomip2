# Load MOM6 ocean data for two months of year for MISOMIP2 processing
# use load_ocean_data_mom6.py
###############################
# load modules
import numpy as np
import xarray as xr
import sys
sys.path.append("/g/data/x77/cy8964/MISOMIP2")
import misomip2.preproc as mp
import os
from datetime import datetime
import glob

startTime = datetime.now()
print(startTime)

from dask.distributed import Client
import sys

import logging
logging.captureWarnings(True)
logging.getLogger('py.warnings').setLevel(logging.ERROR)

if __name__ == '__main__':

    ###############################
    # Start a dask cluster with multiple cores
    client = Client(threads_per_worker=1)
    
    # choose a location to save - ideally on gdata
    save_dir = '/scratch/x77/cy8964/misomip2-processing/'
    ###############################
    #### get run count argument that was passed to python script ####
    
    #year = int(sys.argv[1])
    startoutput = int(sys.argv[1])#(year-1980)*12
    endoutput = startoutput + 1

    np.seterr(divide='ignore', invalid='ignore')

    reg='Weddell' # 'Amundsen' or 'Weddell'

    data_dir='/g/data/ik11/outputs/access-rom3-panan/MC_4km_jra_iaf+regionalpanan+isf/'
    missval=9.969209968386869e36 # missing value in created netcdf files
    
    epsfr=1. # cut sea, sea-ice or ice-shelf fractions below epsfr (in %)
             # (useful to avoid extreme values that can occur with very small fractions)
    
    filmis=False  # keep False (may become a deprecitated option)
                  # if True, will fill nans (missing values in input file or values that can't be triangulated) 
                  # through nearest-neighbor interpolation. This can be useful for the edges of non-structured grids.
    
    f_mesh   = [data_dir+'/output'+np.int64(startoutput).astype('str').zfill(3)+'/access-om3.mom6.static.nc']
    f_bathy  = [data_dir+'/output'+np.int64(startoutput).astype('str').zfill(3)+'/access-om3.mom6.static.nc'] 
    fs_gridT = [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.3d.temp.z.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)]+ \
              [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.tob.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] + \
              [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.3d.e.z.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] + \
               [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.3d.salt.z.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)]+ \
              [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.sob.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)]
    fs_gridU = [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.tauuo.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] + \
              [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.3d.uo.z.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)]
    fs_gridV = [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.tauvo.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] + \
              [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.3d.vo.z.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)]
    fs_ice   = [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.cice.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] 
    fs_is = [data_dir+'/output'+np.int64(startoutput).astype('str').zfill(3)+'/access-om3.mom6.2d.h_shelf.1mon.mean.*.nc' ]
    fs_SBC   = [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.zos.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] + \
            [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.mass_flux.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] + \
            [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.exch_vel_t.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] + \
            [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.thermal_driving.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] + \
            [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.haline_driving.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] + \
            [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.hfds.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] + \
            [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.fsitherm.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] + \
            [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.wfo.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] + \
            [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.ficeberg.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] + \
            [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.salt_flux_added.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)] 
    fs_BSF = [data_dir+'/output'+month.astype('str').zfill(3)+'/access-om3.mom6.2d.umo_2d.1mon.mean.*.nc' for month in np.arange(startoutput,endoutput+1)]
            
    oce = mp.load_oce_mod_mom6( file_mesh_mask=[glob.glob(a)[0] for a in f_mesh], 
                                file_bathy=[glob.glob(a)[0] for a in f_bathy], 
                                files_gridT=[glob.glob(a)[0] for a in fs_gridT],
                                files_gridU=[glob.glob(a)[0] for a in fs_gridU], 
                                files_gridV=[glob.glob(a)[0] for a in fs_gridV], 
                                files_SBC=[glob.glob(a)[0] for a in fs_SBC],
                                files_ice=[glob.glob(a)[0] for a in fs_ice],
                                files_is = [glob.glob(a)[0] for a in fs_is],
                                files_BSF=[glob.glob(a)[0] for a in fs_BSF],
                                rho0=1035.0, teos10=False,parallel=True,
                                chunks = {'xh':1080,'yh':360,'xq':1080, 'yq':360,'yT':360,'xT':1080,'time':1} )
    print('Saving to ',save_dir+'oce_'+str(startoutput)+'_'+str(endoutput)+'.nc')
    
    oce.to_netcdf(save_dir+'oce_'+str(startoutput)+'_'+str(endoutput)+'.nc',
                  unlimited_dims="time")

    model = 'MOM6'; abc='a'; exp='OceanW-hind'; institute = 'ANU'


    # copy from examples/interpolate_to_common_grid_oce.py
    
    #--------------------------------------------------------------------------
    # 2- Global attributes of output netcdf :
    
    def put_global_attrs(ds,experiment='OceanW-hind', avg_hor_res_73S=0.0, original_sim_name='TBD', ocean_model='TBD', institute='TBD', \
                         original_min_lat=-90.0, original_max_lat=-37.5, original_min_lon=-180.0, original_max_lon=180.0):
       """ Put global attributes to the ds xarray dataset
    
       """
       ds.attrs['project'] = 'MISOMIP2'
       ds.attrs['contact'] = 'Claire Yung <claire.yung@anu.edu.au>'     # Name(s) of the person(s) who produced the simulation <email>
       ds.attrs['institute'] = 'ANU'#'Australian National University and ACCESS-NRI'  # Name of the institute(s) that produced the simulation 
                                                          #     (use ``-'' to separate multiple entities)
       ds.attrs['computing_facility'] = 'Gadi (National Computational Infrastructure, Australia)'             # Computing center where the simulation was run
       ds.attrs['interpolation_method'] = 'linear triangular barycentric'  # e.g. 'linear triangular barycentric', 'bi-linear',
                                                                           #      'nearest-neighbor', 'conservative', ...       
       ds.attrs['ocean_model'] = 'MOM6'#'ACCESS-OM3, with ocean component MOM6 (Adcroft et al. 2019) commit 851c39cb89629793cfb16c3c71d88b5d3ba526c6'              # Model name and version
       ds.attrs['reference'] = 'CONTROL simulation in Yung et al. 2026. https://github.com/claireyung/mom6-panAn-iceshelf-tools'                      # Main publication or website describing the simulation or a similar one
       ds.attrs['original_sim_name'] = 'access-rom3-panan/MC_4km_jra_iaf+regionalpanan+isf'  # Original simulation name (to keep track of what was used in MISOMIP2)
       ds.attrs['experiment'] = experiment                # MISOMIP2 experiment; e.g. 'Ocean-A1', 'Ocean-W1', 'Ocean-A2', ...
       ds.attrs['bathymetry'] = 'Merge of GEBCO 2024 and Charrassin et al. (2025)'                     # Bathymetry dataset (specify exact version), e.g. `BedMachine-1.33', `Bedmap2', 
                                                          #    `RTopo-2.0.4', `Merge of Millan et al. (2017) and Bedmap2'
       ds.attrs['ice_shelf_draft'] = 'Charrassin et al. (2025)'                # Dataset for the depth of ice-shelf base (similar to previous)
       ds.attrs['atmosphere'] = 'JRA55do (Tsujino et al. 2018)'                     # Atmospheric forcing, with a reference, e.g., `ERA5 (Hersbach et al. 2020)',
                                                          #    `ERAint (Dee et al. 2011)', `JRA55do (Tsujino et al. 2018)',
                                                          #    `MARv3.9.3 (Donat-Magnin et al. 2020)'
       ds.attrs['iceberg'] = 'Prescribed freshwater and latent heat (JRA55do (Tsujino et al. 2018) fluxes, distributed at grid cells near coast/ice shelf front edge'                        # Method used to account for melting icebergs, with a reference,
                                                          #     e.g. `None', `Lagrangian model (Martin and Adcroft 2010)',
                                                          #          `Prescribed freshwater (Merino et al. 2016)',
                                                          #          `Prescribed Freshwater and Heat (Merino et al. 2016)' 
       ds.attrs['sea_ice'] = 'CICE6 model (Hunke et al. 2025)'                        # Method used to simulate or prescribe the ocean--sea-ice interaction,
                                                          #     with a reference, e.g. 'Dynamics-Thermodynamics Model (Hibler 1979)',
                                                          #                            'Thermodynamics Model (Bitz and Lipscomb 1999)',
                                                          #                            'Prescribed Freshwater and Heat'
       ds.attrs['ocean_lateral_bdy'] = 'Simulation (ACCESS-OM2-01 1990-1991 conditions, second year since WOA 2013 initialisation, Kiss et al. 2020)'              # Type of lateral boundary conditions, e.g. 'Simulation (Merino et al. 2018)',
                                                          #     'Reanalysis (Mazloff et al. 2016)', 'Observations (Locarnini et al. 2018)',
                                                          #     'Corrected simulation (explain method)', 'None' 
       ds.attrs['tides'] = 'Parameterised through a non-uniform tidal velocity, with tidal amplitude from TPXO and scaling parameters from Jourdain et al. 2019'                          # Method used to account for the effect of tides on ice-shelf melt, and dataset
                                                          #     e.g. `Barotropic tidal harmonics prescribed at lateral boundaries (CATS)',
                                                          #          `Forced by a tidal potential', `None',
                                                          #          `Parameterized through uniform tidal velocity (utide=0.1 m s-1)',
                                                          #          'Parameterized through non-uniform tidal velocity (FES2012)'
       ds.attrs['vertical_coordinate'] = 'Arbitrary Lagrangian-Eulerian (ALE) with target coordinate z-coordinates in the open ocean and ice-shelf-draft following beneath ice shelves (Stern et al. 2017)'            # e.g. `Geopotential (Z)', `Stretched Geopotential (Zstar)', `Pressure (P)',
                                                          #      `Stretched Pressure (P*)', `Isopycnal', `Terrain-Following (Sigma)',
                                                          #      `Arbitrary Lagrangian-Eulerian (ALE)'
       ds.attrs['is_melt_param'] = '3-equation (stability and velocity-dependent gamma as in Holland and Jenkins (1999), with C_d = 0.007)'                  # Parameterization used to calculate ice-shelf basal melt rates,
                                                          #      e.g. `3-equation (constant gamma)', `3-equation (velocity-dependent gamma)',
                                                          #           `3-equation (stability and velocity-dependent gamma)'
       ds.attrs['eos'] = 'Wright (1997)'                            # Equation of state in original simulation, e.g. `TEOS10', `EOS80', `linear'
       ds.attrs['advection'] = 'Momentum advection is a second-order scheme and tracer advection uses a third-order piecewise parabolic method (Huyhn 1997)'                      # Brief description of the momentum and tracer advection schemes 
                                                          #      (centered, third-order with limiter, etc)
       ds.attrs['horizontal_mixing'] = 'A biharmonic Smagorinsky horizontal viscosity is used. Background horizontal (within model levels) tracer diffusion is set to zero. '              # Brief description of how ``horizontal'' mixing was performed 
                                                          #      (harmonic, biharmonic, etc.; within model levels, along geopotentials, 
                                                          #       along isopycnals, etc.; using the Gent-McWilliams parameterization; etc)
       ds.attrs['vertical_mixing'] = 'A shear-driven turbulent mixing parameterisation is used (Jackson, Hallberg and Legg, 2008). Background vertical diffusivity is 1.5e-5 m^2/s. The minimum background vertical viscosity is 1e-6 m^2/s. Energetic planetary boundary parameterisation is used in the mixed layer (Reichl & Hallberg, 2018).'                # Brief description of how ``vertical'' mixing was performed (constant diffusivity,
                                                          #       k-profile parameterization, etc.; harmonic, biharmonic, etc)
       ds.attrs['convection'] = 'Parameterized using Reichl & Hallberg (2018) planetary boundary layer scheme.'                     # Brief description of the procedure for handling convection, 
                                                          #       e.g. `Explicitly modeled', `Parameterized using enhanced vertical mixing'
       ds.attrs['avg_hor_res_73S'] = avg_hor_res_73S      # Average horizontal resolution (m) at 73degS in the MISOMIP2 domain (average of x and y resolution)
       ds.attrs['original_min_lat'] = original_min_lat    # Minimum latitude of the original domain in [-90:90]
       ds.attrs['original_max_lat'] = original_max_lat    # Minimum latitude of the original domain in [-90:90]
       ds.attrs['original_min_lon'] = original_min_lon    # Minimum longitude of the original domain in [-180:180]
       ds.attrs['original_max_lon'] = original_max_lon    # Minimum longitude of the original domain in [-180:180]
    
    # 3a- Interpolate to common 3d grid :
    print('interpolate to common 3d grid',flush=True)
    
    # Characteristics of MISOMIP 3d grid:
    [lon_miso,lat_miso,dep_miso] = mp.generate_3d_grid_oce(region=reg)
    mlon = np.size(lon_miso)
    mlat = np.size(lat_miso)
    mdep = np.size(dep_miso)
    lon2d_miso, lat2d_miso = np.meshgrid( lon_miso, lat_miso )
    lon_miso1d = np.reshape( lon2d_miso, mlon*mlat )
    lat_miso1d = np.reshape( lat2d_miso, mlon*mlat )
    
    # model coordinates:
    lonT=oce.lonT ; lonU=oce.lonU ; lonV=oce.lonV
    latT=oce.latT ; latU=oce.latU ; latV=oce.latV
    
    # scale factor to interpolate in the (lon*cos(<lat>),lat) space:
    aa = np.cos( np.mean(lat_miso)*np.pi/180. )
    
    # Some quantities needed to define the global attributes: #CLAIRE
    res_73S = 0.5 * (  oce.dxT.where(  (latT < -72.5) & (latT >= -73.5) & (lonT >= lon_miso.min()) & (lonT <= lon_miso.max()) \
                                     & (oce.LEVOFT.isel(z=0)>99.)  & (oce.LEVOFT.isel(z=0)<1e18) ).mean().values \
                     + oce.dyT.where(  (latT < -72.5) & (latT >= -73.5) & (lonT >= lon_miso.min()) & (lonT <= lon_miso.max()) \
                                     & (oce.LEVOFT.isel(z=0)>99.) & (oce.LEVOFT.isel(z=0)<1e18) ).mean().values )
    print('Average horizontal resolution at 73S :',res_73S)
    
    # 3d sea fraction
    LEVOFT=oce.LEVOFT
    LEVOFU=oce.LEVOFU
    LEVOFV=oce.LEVOFV
    # 2d sea fraction (including under-ice-shelf seas)
    seafracT=LEVOFT.max('z',skipna=True) # 100 if any ocean point, including under ice shelves, =0 or =nan if no ocean point
    seafracU=LEVOFU.max('z',skipna=True)
    seafracV=LEVOFV.max('z',skipna=True)
    # 2d sea fraction (NOT including under-ice-shelf seas)
    openseafracT=LEVOFT.isel(z=0) # 100 only if open ocean point
    openseafracU=LEVOFU.isel(z=0)
    openseafracV=LEVOFV.isel(z=0)
    
    mtime = oce.time.size
    yeari = str(oce.time.isel(time=0).dt.year.values)
    yearf = str(oce.time.isel(time=mtime-1).dt.year.values)
    monthi = str(oce.time.isel(time=0).dt.month.values).zfill(2)
    monthf = str(oce.time.isel(time=mtime-1).dt.month.values).zfill(2)
    period = yeari+monthi+'-'+yearf+monthf
    print('Data from '+monthi+'-'+yeari+' to '+monthf+'-'+yearf)
    
    # mask showing the original domain on the MISOMIP grid (=nan where interpolation of any of T, U, V grid is nan, =1 elsewhere):
    DOMMSK_miso =               mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.DOMMSKT )
    DOMMSK_miso = DOMMSK_miso + mp.horizontal_interp( latU, lonU*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.DOMMSKU )
    DOMMSK_miso = DOMMSK_miso + mp.horizontal_interp( latV, lonV*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.DOMMSKV )
    
    # horizontal interpolation grid roation angles :
    theT = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.thetaT )
    theU = mp.horizontal_interp( latU, lonU*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.thetaU )
    theV = mp.horizontal_interp( latV, lonV*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.thetaV )
    
    # Sea Area Percentage at the Surface on MISOMIP grid (in [0-100], =nan beyond model domain)
    SFTOF_miso = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.SFTOF )
    SFTOF_miso[ np.isnan(DOMMSK_miso) ] = 0.e0 # will update to missval at the end of the calculation
    
    # Ice shelf fraction on MISOMIP grid (in [0-100], =nan beyond model domain)
    SFTFLF_miso = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.SFTFLF )
    SFTFLF_miso[ np.isnan(DOMMSK_miso) ] = 0.e0 # will update to missval at the end of the calculation
    
    # Depth of floating ice on MISOMIP grid (ice-shelf draft)
    DEPFLF_miso = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.DEPFLF, weight=oce.SFTFLF, skipna=True, filnocvx=filmis, threshold=epsfr )
    DEPFLF_miso[ np.isnan(DOMMSK_miso) | np.isnan(DEPFLF_miso) ] = 0.e0  # will be replaced with missval later on
    
    # Ocean depth on MISOMIP grid (=nan where land or grounded ice or beyond model domain)
    DEPTHO_miso = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.DEPTHO, weight=seafracT, skipna=True, filnocvx=filmis, threshold=epsfr )
    DEPTHO_miso[ np.isnan(DOMMSK_miso) | np.isnan(DEPTHO_miso) ] = 0.e0 # will be replaced with missval later on
    
    # Sea area fraction at each vertical level on the MISOMIP grid: 
    tmp_LEVT = LEVOFT.interp(z=dep_miso) # extrapolate to avoid nan at surface but better to
    tmp_LEVU = LEVOFU.interp(z=dep_miso) # bring first level to zero when loading model data.
    tmp_LEVV = LEVOFV.interp(z=dep_miso)
    
    LEVOF_miso = np.zeros((mdep,mlat,mlon))
    for kk in np.arange(mdep):
      print(f"\rDepth {kk+1}/{mdep} ({100*(kk+1)/mdep:.1f}%)",end="", flush=True)
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, tmp_LEVT[kk,:] )
      tzz[ np.isnan(DOMMSK_miso) ] = np.nan # will update to missval at the end of the calculation
      LEVOF_miso[kk,:,:] = tzz
    print()
    
    print('begin vertical interp of fields',flush=True)
    # vertical interpolation of T, S, U, V to common vertical grid :
    
    tmpxS = LEVOFT*oce.SO
    tmp_SS = tmpxS.interp(z=dep_miso) / tmp_LEVT
    
    tmpxT = LEVOFT*oce.THETAO
    tmp_TT = tmpxT.interp(z=dep_miso) / tmp_LEVT
    
    tmpxU = LEVOFU*oce.UX
    tmp_UX = tmpxU.interp(z=dep_miso) / tmp_LEVU
    
    tmpxV = LEVOFV*oce.VY
    tmp_VY = tmpxV.interp(z=dep_miso) / tmp_LEVV
    
    #----- interpolation of time-varying fields:
    print('interpolation of time-varying fields',flush=True)
    
    SO_miso     = np.zeros((mtime,mdep,mlat,mlon)) + missval
    THETAO_miso = np.zeros((mtime,mdep,mlat,mlon)) + missval
    UO_miso     = np.zeros((mtime,mdep,mlat,mlon)) + missval
    VO_miso     = np.zeros((mtime,mdep,mlat,mlon)) + missval
    ZOS_miso    = np.zeros((mtime,mlat,mlon)) + missval
    TOB_miso    = np.zeros((mtime,mlat,mlon)) + missval
    SOB_miso    = np.zeros((mtime,mlat,mlon)) + missval
    FLANDICE_miso = np.zeros((mtime,mlat,mlon)) + missval
    FSITHERM_miso = np.zeros((mtime,mlat,mlon)) + missval
    LIBMASSBFFL_miso = np.zeros((mtime,mlat,mlon)) + missval
    DYDRFLF_miso = np.zeros((mtime,mlat,mlon)) + missval
    THDRFLF_miso = np.zeros((mtime,mlat,mlon)) + missval
    HADRFLF_miso = np.zeros((mtime,mlat,mlon)) + missval
    MSFTBAROT_miso = np.zeros((mtime,mlat,mlon)) + missval
    HFS_miso = np.zeros((mtime,mlat,mlon)) + missval
    WFOAT_miso = np.zeros((mtime,mlat,mlon)) + missval
    WFOCORR_miso = np.zeros((mtime,mlat,mlon)) + missval
    SICONC_miso = np.zeros((mtime,mlat,mlon)) + missval
    SIVOL_miso = np.zeros((mtime,mlat,mlon)) + missval
    SIU_miso = np.zeros((mtime,mlat,mlon)) + missval
    SIV_miso = np.zeros((mtime,mlat,mlon)) + missval
    TAUUO_miso = np.zeros((mtime,mlat,mlon)) + missval
    TAUVO_miso = np.zeros((mtime,mlat,mlon)) + missval
    
    domcond = (np.isnan(DOMMSK_miso))
    
    for ll in np.arange(mtime):
      print(f"\r{ll+1}/{mtime}  Time: {datetime.now():%H:%M:%S}",end="", flush=True)
    
      ## fields interpolated from sea cells ( C/T grid ):
    
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.ZOS.isel(time=ll), \
                                  weight=seafracT, skipna=True, filnocvx=filmis, threshold=epsfr )
      tzz[ np.isnan(tzz) | domcond ] = missval
      ZOS_miso[ll,:,:] = tzz
    
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.TOB.isel(time=ll), \
                                  weight=seafracT, skipna=True, filnocvx=filmis, threshold=epsfr )
      tzz[ np.isnan(tzz) | domcond ] = missval
      TOB_miso[ll,:,:] = tzz
    
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.SOB.isel(time=ll), \
                                  weight=seafracT, skipna=True, filnocvx=filmis, threshold=epsfr )
      tzz[ np.isnan(tzz) | domcond ] = missval
      SOB_miso[ll,:,:] = tzz
    
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.HFS.isel(time=ll), \
                                  weight=seafracT, skipna=True, filnocvx=filmis, threshold=epsfr ) 
      tzz[ np.isnan(tzz) | domcond ] = missval
      HFS_miso[ll,:,:] = tzz
    
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.WFOAT.isel(time=ll), \
                                  weight=seafracT, skipna=True, filnocvx=filmis, threshold=epsfr ) 
      tzz[ np.isnan(tzz) | domcond ] = missval
      WFOAT_miso[ll,:,:] = tzz
    
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.FLANDICE.isel(time=ll), \
                                  weight=seafracT, skipna=True, filnocvx=filmis, threshold=epsfr )
      tzz[ np.isnan(tzz) | domcond ] = missval
      FLANDICE_miso[ll,:,:] = tzz
    
      ## fields interpolated from sea cells ( U & V grids ):
    
      tzz = mp.horizontal_interp( latU, lonU*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.MSFTBAROT.isel(time=ll), \
                                  weight=seafracU, skipna=True, filnocvx=filmis, threshold=epsfr ) 
      tzz[ np.isnan(tzz) | domcond ] = missval
      MSFTBAROT_miso[ll,:,:] = tzz
    
      TAUX_notrot = mp.horizontal_interp( latU, lonU*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.TAUX.isel(time=ll), \
                                          weight=seafracU, skipna=True, filnocvx=filmis, threshold=epsfr )
      TAUY_notrot = mp.horizontal_interp( latV, lonV*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.TAUY.isel(time=ll), \
                                          weight=seafracV, skipna=True, filnocvx=filmis, threshold=epsfr )
      TAUUO_miso[ll,:,:] = TAUX_notrot * np.cos(theU) - TAUY_notrot * np.sin(theV) # rotated to zonal
      TAUVO_miso[ll,:,:] = TAUY_notrot * np.cos(theV) + TAUX_notrot * np.sin(theU) # rotated to meridional
      TAUUO_miso[ ( np.isnan(TAUUO_miso) ) | domcond ] = missval
      TAUVO_miso[ ( np.isnan(TAUVO_miso) ) | domcond ] = missval
    
      ## fileds interpolated from open-sea cells :
    
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.WFOCORR.isel(time=ll), \
                                  weight=openseafracT, skipna=True, filnocvx=filmis, threshold=epsfr )
      tzz[ np.isnan(tzz) | domcond ] = missval
      WFOCORR_miso[ll,:,:] = tzz
    
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.SICONC.isel(time=ll), \
                                  weight=openseafracT, skipna=True, filnocvx=filmis, threshold=epsfr )
      tzz[ np.isnan(tzz) | domcond ] = np.nan # will be updated to missval later on
      SICONC_miso[ll,:,:] = tzz
    
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.SIVOL.isel(time=ll), \
                                  weight=openseafracT, skipna=True, filnocvx=filmis, threshold=epsfr )
      tzz[ np.isnan(tzz) | domcond ] = missval
      SIVOL_miso[ll,:,:] = tzz
    
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.FSITHERM.isel(time=ll), \
                                  weight=openseafracT, skipna=True, filnocvx=filmis, threshold=epsfr )
      tzz[ np.isnan(tzz) | domcond ] = missval
      FSITHERM_miso[ll,:,:] = tzz
    
      ## fileds interpolated from ice-shelf cells :
    
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.LIBMASSBFFL.isel(time=ll), \
                                  weight=oce.SFTFLF, skipna=True, filnocvx=filmis, threshold=epsfr )
      tzz[ np.isnan(tzz) | domcond ] = missval
      LIBMASSBFFL_miso[ll,:,:] = tzz
    
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.DYDRFLF.isel(time=ll), \
                                  weight=oce.SFTFLF, skipna=True, filnocvx=filmis, threshold=epsfr ) 
      tzz[ np.isnan(tzz) | domcond ] = missval
      DYDRFLF_miso[ll,:,:] = tzz
    
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.THDRFLF.isel(time=ll), \
                                  weight=oce.SFTFLF, skipna=True, filnocvx=filmis, threshold=epsfr ) 
      tzz[ np.isnan(tzz) | domcond ] = missval
      THDRFLF_miso[ll,:,:] = tzz
    
      tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.HADRFLF.isel(time=ll), \
                                  weight=oce.SFTFLF, skipna=True, filnocvx=filmis, threshold=epsfr ) 
      tzz[ np.isnan(tzz) | domcond ] = missval
      HADRFLF_miso[ll,:,:] = tzz
    
      ## fileds interpolated from sea-ice cells :
    
      SIUX_notrot = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.SIUX.isel(time=ll), \
                                          weight=oce.SICONC.isel(time=ll), skipna=True, filnocvx=filmis, threshold=epsfr )
      SIVY_notrot = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, oce.SIVY.isel(time=ll), \
                                          weight=oce.SICONC.isel(time=ll), skipna=True, filnocvx=filmis, threshold=epsfr )
      SIU_miso[ll,:,:] = SIUX_notrot * np.cos(theT) - SIVY_notrot * np.sin(theT) # rotated to zonal
      SIV_miso[ll,:,:] = SIVY_notrot * np.cos(theT) + SIUX_notrot * np.sin(theT) # rotated to meridional
      SIU_miso [ np.isnan(SIU_miso) | domcond ] = missval
      SIV_miso [ np.isnan(SIV_miso) | domcond ] = missval
    
      for kk in np.arange(mdep):
    
        # horizontal interpolations of time-varying 3d fields to common horizontal grid : 
    
        condkk = ( (LEVOF_miso[kk,:,:] < epsfr) | domcond )
    
        tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, tmp_SS.isel(time=ll,z=kk), \
                                    weight=tmp_LEVT.isel(z=kk), skipna=True, filnocvx=filmis, threshold=epsfr )
        tzz[ condkk | (np.isnan(tzz)) ] = missval
        SO_miso[ll,kk,:,:] = tzz
    
        tzz = mp.horizontal_interp( latT, lonT*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, tmp_TT.isel(time=ll,z=kk), \
                                    weight=tmp_LEVT.isel(z=kk), skipna=True, filnocvx=filmis, threshold=epsfr )
        tzz[ condkk | (np.isnan(tzz)) ] = missval
        THETAO_miso[ll,kk,:,:] = tzz
    
        UX_notrot = mp.horizontal_interp( latU, lonU*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, tmp_UX.isel(time=ll,z=kk), \
                                          weight=tmp_LEVU.isel(z=kk), skipna=True, filnocvx=filmis, threshold=epsfr )
        VY_notrot = mp.horizontal_interp( latV, lonV*aa, mlat, mlon, lat_miso1d, lon_miso1d*aa, tmp_VY.isel(time=ll,z=kk), \
                                          weight=tmp_LEVV.isel(z=kk), skipna=True, filnocvx=filmis, threshold=epsfr )
        tzz = UX_notrot * np.cos(theU) - VY_notrot * np.sin(theV) # rotated to zonal
        tzz[ condkk | (np.isnan(tzz)) ] = missval
        UO_miso[ll,kk,:,:] = tzz
        tzz = VY_notrot * np.cos(theV) + UX_notrot * np.sin(theU) # rotated to meridional
        tzz[ condkk | (np.isnan(tzz)) ] = missval
        VO_miso[ll,kk,:,:] = tzz
    
    
    LEVOF_miso[ np.isnan(LEVOF_miso) ] = missval
    SFTFLF_miso[ np.isnan(SFTFLF_miso) | np.isnan(DOMMSK_miso) ] = missval 
    SFTOF_miso[ np.isnan(SFTOF_miso) | np.isnan(DOMMSK_miso) ] = missval 
    SICONC_miso[ np.isnan(SICONC_miso) ] = missval
    DEPFLF_miso[ (DEPFLF_miso==0.e0) | np.isnan(DOMMSK_miso) ] = missval
    DEPTHO_miso[ np.isnan(DOMMSK_miso) | (DEPTHO_miso==0.) ] = missval
    
    #--------------------------------------------------------------------------
    # 3b- Create new xarray dataset and save to netcdf
    print('making new xarray dataset',flush=True)
    
    dsmiso3d = xr.Dataset(
        {
        "so":        (["time", "lev", "lat", "lon"], np.float32(SO_miso)),
        "thetao":    (["time", "lev", "lat", "lon"], np.float32(THETAO_miso)),
        "uo":        (["time", "lev", "lat", "lon"], np.float32(UO_miso)),
        "vo":        (["time", "lev", "lat", "lon"], np.float32(VO_miso)),
        "tauuo":     (["time", "lat", "lon"], np.float32(TAUUO_miso)),
        "tauvo":     (["time", "lat", "lon"], np.float32(TAUVO_miso)),
        "zos":       (["time", "lat", "lon"], np.float32(ZOS_miso)),
        "tob":       (["time", "lat", "lon"], np.float32(TOB_miso)),
        "sob":       (["time", "lat", "lon"], np.float32(SOB_miso)),
        "libmassbffl": (["time", "lat", "lon"], np.float32(LIBMASSBFFL_miso)),
        "dydrflf":   (["time", "lat", "lon"], np.float32(DYDRFLF_miso)),
        "thdrflf":   (["time", "lat", "lon"], np.float32(THDRFLF_miso)),
        "hadrflf":   (["time", "lat", "lon"], np.float32(HADRFLF_miso)),
        "msftbarot": (["time", "lat", "lon"], np.float32(MSFTBAROT_miso)),
        "flandice":  (["time", "lat", "lon"], np.float32(FLANDICE_miso)),
        "fsitherm":  (["time", "lat", "lon"], np.float32(FSITHERM_miso)),
        "hfs":       (["time", "lat", "lon"], np.float32(HFS_miso)),
        "wfoat":     (["time", "lat", "lon"], np.float32(WFOAT_miso)),
        "wfocorr":   (["time", "lat", "lon"], np.float32(WFOCORR_miso)),
        "siconc":    (["time", "lat", "lon"], np.float32(SICONC_miso)),
        "sivol":     (["time", "lat", "lon"], np.float32(SIVOL_miso)),
        "siu":       (["time", "lat", "lon"], np.float32(SIU_miso)),
        "siv":       (["time", "lat", "lon"], np.float32(SIV_miso)),
        "levof":     (["lev", "lat", "lon"], np.float32(LEVOF_miso)),
        "sftof":     (["lat", "lon"], np.float32(SFTOF_miso)),
        "sftflf":    (["lat", "lon"], np.float32(SFTFLF_miso)),
        "depflf":    (["lat", "lon"], np.float32(DEPFLF_miso)),
        "deptho":    (["lat", "lon"], np.float32(DEPTHO_miso)),
        },
        coords={
        "lon":np.float32(lon_miso),
        "lat": np.float32(lat_miso),
        "lev": np.float32(dep_miso),
        "time": oce.time.values
        },
    )
    
    mp.add_standard_attributes_oce(dsmiso3d,miss=missval)
    put_global_attrs(dsmiso3d, #experiment=exp, 
                     avg_hor_res_73S=res_73S, 
                     #original_sim_name=original_sim_name, \
                     #ocean_model=model, institute=institute, \
                     original_min_lat=oce.attrs.get('original_minlat'),original_max_lat=oce.attrs.get('original_maxlat'),\
                     original_min_lon=oce.attrs.get('original_minlon'),original_max_lon=oce.attrs.get('original_maxlon') )
    
    file_miso3d = 'Oce3d_'+institute+'_'+model+'_'+abc+'_'+exp+'_'+period+'.nc'
    
    print('Creating ',file_miso3d)
    dsmiso3d.to_netcdf(save_dir+file_miso3d,unlimited_dims="time")
    
    del dsmiso3d
    del SO_miso, THETAO_miso, UO_miso, VO_miso, ZOS_miso, LIBMASSBFFL_miso, DYDRFLF_miso, THDRFLF_miso, HADRFLF_miso, MSFTBAROT_miso, FLANDICE_miso
    del HFS_miso, WFOAT_miso, WFOCORR_miso, SICONC_miso, SIVOL_miso, SIU_miso, SIV_miso, FSITHERM_miso
    
    print('  Execution time: ',datetime.now() - startTime)
    
    #--------------------------------------------------------------------------
    #--------------------------------------------------------------------------
    
    for sec in np.arange(1,5,1,dtype='int'):
    
       #--------------------------------------------------------------------------
       # 4a- Interpolate to common section :
       
       # Characteristics of MISOMIP section grid:
       [lon_sect1d,lat_sect1d,dep_sect] = mp.generate_section_grid_oce(region=reg,section=sec)
       mlonlatsec = np.size(lon_sect1d)
       mdepsect = np.size(dep_sect)
       
       # Reduce input data size
       #   NB: for some reason, a small number of points (low wdeg below) may lead to the following error here:
       #       scipy.spatial.qhull.QhullError: QH6214 qhull input error: not enough points(1) to construct initial simplex (need 4) 
       #       If it happens with your dataset, increase it further...
       wdeg = 3.0 * res_73S / ( 6.37e6 * np.pi / 180. )
       lonmin_sect = np.min( lon_sect1d ) - wdeg/aa
       lonmax_sect = np.max( lon_sect1d ) + wdeg/aa
       latmin_sect = np.min( lat_sect1d ) - wdeg
       latmax_sect = np.max( lat_sect1d ) + wdeg
       
       cond_secT=( (oce.latT>latmin_sect) & (oce.latT<latmax_sect) & (oce.lonT>lonmin_sect) & (oce.lonT<lonmax_sect) )
       
       # model coordinates:
       lonT=oce.lonT.where(cond_secT,drop=True)
       latT=oce.latT.where(cond_secT,drop=True)
       
       # 3d sea fraction
       LEVOFT=oce.LEVOFT.where(cond_secT,drop=True)
       # 2d sea fraction (including under-ice-shelf seas)
       seafracT=LEVOFT.max('z',skipna=True) # 100 if any ocean point, including under ice shelves, =0 or =nan if no ocean point
       # 2d sea fraction (NOT including under-ice-shelf seas)
       openseafracT=LEVOFT.isel(z=0) # 100 only if open ocean point
       
       # mask showing the original domain (nan where interpolation of any of T, U, V grid is nan):
       DOMMSK_sect = np.squeeze(mp.horizontal_interp( latT, lonT*aa, mlonlatsec, 1, lat_sect1d, lon_sect1d*aa, oce.DOMMSKT.where(cond_secT,drop=True) ))
       
       domcond = ( np.isnan(DOMMSK_sect) )
       
       # Depth of floating ice on MISOMIP grid (ice-shelf draft)
       DEPFLF_sect = np.squeeze( mp.horizontal_interp( latT, lonT*aa, mlonlatsec, 1, lat_sect1d, lon_sect1d*aa, oce.DEPFLF.where(cond_secT,drop=True), \
                                                       weight=oce.SFTFLF.where(cond_secT,drop=True), \
                                                       skipna=True, filnocvx=filmis, threshold=epsfr ) )
       DEPFLF_sect[ domcond | np.isnan(DEPFLF_sect) ] = 0.e0 # will be replaced with missval later on
       
       # Ocean depth on MISOMIP grid (=nan where land or grounded ice or beyond model domain)
       DEPTHO_sect = np.squeeze( mp.horizontal_interp( latT, lonT*aa, mlonlatsec, 1, lat_sect1d, lon_sect1d*aa, oce.DEPTHO.where(cond_secT,drop=True), \
                                                       weight=seafracT, skipna=True, filnocvx=filmis, threshold=epsfr ))
       DEPTHO_sect[ domcond | np.isnan(DEPTHO_sect) ] = 0.e0 # will be replaced with missval later on
       
       # vertical then horizontal interpolation of constant 3d fields to common grid :
       tmp_LEVT = LEVOFT.interp(z=dep_sect)
       
       LEVOF_sect = np.zeros((mdepsect,mlonlatsec))
       for kk in np.arange(mdepsect):
         tzz = np.squeeze( mp.horizontal_interp( latT, lonT*aa, mlonlatsec, 1, lat_sect1d, lon_sect1d*aa, tmp_LEVT[kk,:] ) )
         tzz[ domcond ] = missval
         LEVOF_sect[kk,:] = tzz
       
       # vertical interpolation of T, S, U, V to common vertical grid :
       tmpxS = LEVOFT*oce.SO.where(cond_secT,drop=True)
       tmp_SS = tmpxS.interp(z=dep_sect) / tmp_LEVT
       tmpxT = LEVOFT*oce.THETAO.where(cond_secT,drop=True)
       tmp_TT = tmpxT.interp(z=dep_sect) / tmp_LEVT
       SO_sect     = np.zeros((mtime,mdepsect,mlonlatsec)) + missval
       THETAO_sect = np.zeros((mtime,mdepsect,mlonlatsec)) + missval
       
       for ll in np.arange(mtime):
         for kk in np.arange(mdepsect):
       
           condkk = ( (LEVOF_sect[kk,:] < epsfr) | domcond )
      
           tzz = np.squeeze(mp.horizontal_interp( latT, lonT*aa, mlonlatsec, 1, lat_sect1d, lon_sect1d*aa, tmp_SS.isel(time=ll,z=kk), \
                                                  weight=tmp_LEVT.isel(z=kk), skipna=True, filnocvx=filmis, threshold=epsfr ) )
           tzz[ condkk | (np.isnan(tzz)) ] = missval
           SO_sect[ll,kk,:] = tzz
       
           tzz = np.squeeze(mp.horizontal_interp( latT, lonT*aa, mlonlatsec, 1, lat_sect1d, lon_sect1d*aa, tmp_TT.isel(time=ll,z=kk), \
                                                  weight=tmp_LEVT.isel(z=kk), skipna=True, filnocvx=filmis, threshold=epsfr ) )
           tzz[ condkk | (np.isnan(tzz)) ] = missval
           THETAO_sect[ll,kk,:] = tzz
       
       DEPFLF_sect[ DEPFLF_sect==0.e0 ] = missval
       DEPTHO_sect[ np.isnan(DOMMSK_sect) | (DEPTHO_sect==0.) ] = missval
       
       #--------------------------------------------------------------------------
       # 4b- Create new xarray dataset and save to netcdf
       
       dssect = xr.Dataset(
           {
           "so":        (["time", "lev", "x"], np.float32(SO_sect)),
           "thetao":    (["time", "lev", "x"], np.float32(THETAO_sect)),
           "levof":     (["lev", "x"], np.float32(LEVOF_sect)),
           "lon": (["x"], np.float32(lon_sect1d)),
           "lat": (["x"], np.float32(lat_sect1d)),
           },
           coords={
           "lev": np.float32(dep_sect),
           "time": oce.time.values
           },
       )
       
       mp.add_standard_attributes_oce(dssect,miss=missval)
       put_global_attrs(dssect, #experiment=exp, 
                        avg_hor_res_73S=res_73S, #original_sim_name=original_sim_name,\
                        ocean_model=model, institute=institute, \
                        original_min_lat=oce.attrs.get('original_minlat'),original_max_lat=oce.attrs.get('original_maxlat'),\
                        original_min_lon=oce.attrs.get('original_minlon'),original_max_lon=oce.attrs.get('original_maxlon') )
       
       file_sect = 'OceSec'+str(sec)+'_'+institute+'_'+model+'_'+abc+'_'+exp+'_'+period+'.nc'
       
       print('Creating ',file_sect)
       dssect.to_netcdf(save_dir+file_sect,unlimited_dims="time")
       
       del dssect
       del SO_sect, THETAO_sect
       
    print('  Execution time: ',datetime.now() - startTime)
    
    #--------------------------------------------------------------------------
    #--------------------------------------------------------------------------
    
    for moor in np.arange(1,9,1,dtype='int'):
    
       #--------------------------------------------------------------------------
       # 5a- Interpolate to common mooring location :
       #
       #     For this mooring, we take the closest grid point that is not under 
       #     an ice shelf (observational mooring in front of a real ice shelf
       #     can be located in the cavity of some models).
       
       # Characteristics of MISOMIP mooring:
       [lon_moor0d,lat_moor0d,dep_moor] = mp.generate_mooring_grid_oce(region=reg,mooring=moor)
       mdepmoor = np.size(dep_moor)
       
       # Reduce input data size
       #   NB: for some reason, a small number of points (low wdeg below) may lead to the following error here:
       #       scipy.spatial.qhull.QhullError: QH6214 qhull input error: not enough points(1) to construct initial simplex (need 4) 
       #       If it happens with your dataset, increase it further...
       wdeg = 3.0 * res_73S / ( 6.37e6 * np.pi / 180. )
       lonmin_moor = np.min( lon_moor0d ) - wdeg/aa
       lonmax_moor = np.max( lon_moor0d ) + wdeg/aa
       latmin_moor = np.min( lat_moor0d ) - wdeg
       latmax_moor = np.max( lat_moor0d ) + wdeg
       
       cond_mooT=( (oce.latT>latmin_moor) & (oce.latT<latmax_moor) & (oce.lonT>lonmin_moor) & (oce.lonT<lonmax_moor) )
       
       # model coordinates:
       lonT=oce.lonT.where(cond_mooT,drop=True)
       latT=oce.latT.where(cond_mooT,drop=True)
       
       # 3d sea fraction
       LEVOFT=oce.LEVOFT.where(cond_mooT,drop=True)
       # 2d sea fraction (including under-ice-shelf seas)
       seafracT=LEVOFT.max('z',skipna=True) # 100 if any ocean point, including under ice shelves, =0 or =nan if no ocean point
       # 2d sea fraction (NOT including under-ice-shelf seas)
       openseafracT=LEVOFT.isel(z=0) # 100 only if open ocean point

       fracT = seafracT
       
       mtime = np.shape(oce.SO)[0]
       
       # Coordinates of closest point:
       condclo = ((lat_moor0d-latT.where(fracT >= 50.))**2+((lon_moor0d-lonT.where(fracT >= 50.))*aa)**2)
       is_all_nan = condclo.isnull().all().item()
    
       if is_all_nan:
    
          print('No points found for mooring',moor,' at (lon,lat) = ',lon_moor0d,lat_moor0d)
          print('   >>>>>>> file not saved')
    
       else:
    
          indxy = ((lat_moor0d-latT.where(fracT >= 50.))**2+((lon_moor0d-lonT.where(fracT >= 50.))*aa)**2).argmin()
          
          print('Closest point of open sea near mooring of (lon,lat) = ',lon_moor0d,lat_moor0d)
          print('   is at model point of (lon,lat) = ',lonT.where(fracT >= 50.).isel(sxy=indxy).values, latT.where(fracT >= 50.).isel(sxy=indxy).values )
          DOMMSK_moor = oce.DOMMSKT.where(cond_mooT,drop=True).where(fracT >= 50.).isel(sxy=indxy).values
          
          tmp_LEVT = LEVOFT.interp(z=dep_moor)
          LEVOF_moor = np.zeros((mdepmoor))
          for kk in np.arange(mdepmoor):
            tzz = tmp_LEVT[kk,:].where(fracT >= 50.).isel(sxy=indxy).values
            tzz[ np.isnan(DOMMSK_moor) ] = missval
            LEVOF_moor[kk] = tzz
          
          # vertical interpolation of T, S to common vertical grid :
          tmpxS = LEVOFT*oce.SO.where(cond_mooT,drop=True)
          tmp_SS = tmpxS.interp(z=dep_moor) / tmp_LEVT
          tmpxT = LEVOFT*oce.THETAO.where(cond_mooT,drop=True)
          tmp_TT = tmpxT.interp(z=dep_moor) / tmp_LEVT
          SO_moor     = np.zeros((mtime,mdepmoor)) + missval
          THETAO_moor = np.zeros((mtime,mdepmoor)) + missval
          
          for ll in np.arange(mtime):
            for kk in np.arange(mdepmoor):
          
              tzz = tmp_SS[kk,:,ll].where(fracT >= 50.).isel(sxy=indxy).values
              tzz[ np.isnan(DOMMSK_moor) | (LEVOF_moor[kk] < epsfr) ] = missval
              SO_moor[ll,kk] = tzz
          
              tzz = tmp_TT[kk,:,ll].where(fracT >= 50.).isel(sxy=indxy).values
              tzz[ np.isnan(DOMMSK_moor) | (LEVOF_moor[kk] < epsfr) ] = missval
              THETAO_moor[ll,kk] = tzz
          
          #--------------------------------------------------------------------------
          # 5b- Create new xarray dataset and save to netcdf
          
          dsmoor = xr.Dataset(
              {
              "so":        (["time", "lev"], np.float32(SO_moor)),
              "thetao":    (["time", "lev"], np.float32(THETAO_moor)),
              "levof":     (["lev"], np.float32(LEVOF_moor)),
              },
              coords={
              "lev": np.float32(dep_moor),
              "time": oce.time.values
              },
          )
          
          mp.add_standard_attributes_oce(dsmoor,miss=missval)
          put_global_attrs(dsmoor, #experiment=exp, 
                           avg_hor_res_73S=res_73S, #original_sim_name=original_sim_name,\
                           ocean_model=model, institute=institute, \
                           original_min_lat=oce.attrs.get('original_minlat'),original_max_lat=oce.attrs.get('original_maxlat'),\
                           original_min_lon=oce.attrs.get('original_minlon'),original_max_lon=oce.attrs.get('original_maxlon') )
          dsmoor.attrs['mooring_lon'] = np.float32(lon_moor0d)
          dsmoor.attrs['mooring_lat'] = np.float32(lat_moor0d)
          
          file_moor = 'OceMoor'+str(moor)+'_'+institute+'_'+model+'_'+abc+'_'+exp+'_'+period+'.nc'
          
          print('Creating ',file_moor)
          dsmoor.to_netcdf(save_dir+file_moor,unlimited_dims="time")
    
    print('  Execution time: ',datetime.now() - startTime)


