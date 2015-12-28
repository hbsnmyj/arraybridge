/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2015 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/
#include <stdio.h>
#include <sys/types.h>

/* instead of
 * #include "../../scalapackUtil/scalapackTypes.hpp"
 * which is for c++
 * just use the folowing so that the blas emulation is all straight C/Fortran
 */
typedef struct context {
    int nprow, npcol, myrow, mycol;
} context_t ;

/**
 * a verson of the FORTRAN blacs_gridinfo interface that allows
 * the blacs scheme for info about the process grid to function
 * in SciDB even though it is not linked to ScaLAPACK
 * TODO JHM ; This will be replaced at a later milestone in the
 *            DLA/ScaLAPACK project once we decide how we will
 *            deal with multiple contxt-s, which will be required
 *            for multi-user execution
 *
 * NOTE: this is iso C, not C++, to avoid name-mangling, so it will
 *       have the same symbol as the FORTRAN version
 */
void scidb_blacs_gridinfo_(const context_t* contxt, int *nprow, int *npcol,
                                                    int *myrow, int *mycol)
{
    *nprow = contxt->nprow;
    *npcol = contxt->npcol;
    *myrow = contxt->myrow;
    *mycol = contxt->mycol;
}

void scidb_set_blacs_gridinfo_(context_t* contxt, int *nprow, int *npcol,
                                                  int *myrow, int *mycol)
{
    contxt->nprow = *nprow;
    contxt->npcol = *npcol;
    contxt->myrow = *myrow;
    contxt->mycol = *mycol;
}

