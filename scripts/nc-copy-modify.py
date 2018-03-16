#! python
from argparse import ArgumentParser

from netCDF4 import Dataset


def nc_copy(source_fp, dest_fp, time_dimension, time_indices, variables):
    print('nc_copy({source_fp}, {dest_fp}, {time_dimension}, {time_indices}, {variables})'.format(**locals()))

    with Dataset(source_fp) as source:
        with Dataset(dest_fp, mode='w') as dest:

            # Copy global attributes
            for name in source.ncattrs():
                dest.setncattr(name, source.getncattr(name))

            # Create and copy dimensions
            for name, dimension in source.dimensions.items():
                if time_indices and name == time_dimension:
                    size = time_indices[1] - time_indices[0] + 1
                else:
                    size = dimension.size
                print('Copying dimension {} ({})'.format(name, size))
                dest.createDimension(name, size=size)

            # Create and copy variables
            for name, source_variable in source.variables.items():
                # Create variable
                contiguous = False
                chunksizes = None
                # if source_variable.chunking() == 'contiguous':
                #     contiguous = True
                #     chunksizes = None
                # else:
                #     contiguous = False
                #     chunksizes = source_variable.chunking()
                #     print('chunksizes={}'.format(chunksizes))

                print('Copying variable {}'.format(name))
                dest_variable = dest.createVariable(
                    name, source_variable.datatype,
                    dimensions=source_variable.dimensions,
                    **source_variable.filters(),
                    endian=source_variable.endian(),
                    contiguous=contiguous,
                    chunksizes=chunksizes,
                )

                # Copy variable attributes
                for name in source_variable.ncattrs():
                    dest_variable.setncattr(name, source_variable.getncattr(name))

                # Copy variable values
                if (
                        # we want to process this variable
                        (not variables or name in variables)
                        # it has a time dimension
                        and time_dimension in source_variable.dimensions
                        # and time indices are specified
                        and time_indices
                ):
                    print('\tslicing')
                    # copy only a subset of the time dimension
                    slices = [slice(None),] * source_variable.ndim
                    t = source_variable.dimensions.index(time_dimension)
                    slices[t] = slice(time_indices[0], time_indices[1]+1)
                    dest_variable[:] = source_variable[tuple(slices)]
                else:
                    dest_variable[:] = source_variable[:]


if __name__ == '__main__':
    parser = ArgumentParser(
        description='Copy a NetCDF file with some modifications, '
                    'namely selecting a subset of the times for specified '
                    'variables')
    parser.add_argument(
        'source', help='Source file to copy')
    parser.add_argument(
        'dest', help='Destination file')
    parser.add_argument(
        '--time-dimension', dest='time_dimension', default='time',
        help='Time dimension name')
    parser.add_argument(
        '--time-indices', dest='time_indices', default=None,
        help='Time indices')
    parser.add_argument(
        '--variables', default=None, help='Variables')
    args = parser.parse_args()

    nc_copy(
        args.source, args.dest,
        args.time_dimension,
        args.time_indices and [int(i) for i in args.time_indices.split('-')],
        args.variables and args.variables.split(',')
    )
