/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

function onSearchStringChange(idString) {
    var searchString = $('#search').val().toLowerCase();
    //remove the stacktrace
    collapseAllThreadStackTrace(false)
    if (searchString.length == 0) {
        $('tr').each(function() {
            $(this).removeClass('hidden')
        })
    } else {
        $('tr').each(function(){
            if($(this).attr('id') && $(this).attr('id').match(idString) ) {
                var children = $(this).children()
                var found = false
                for (i = 0; i < children.length; i++) {
                    if (children.eq(i).text().toLowerCase().indexOf(searchString) >= 0) {
                        found = true
                    }
                }
                if (found) {
                    $(this).removeClass('hidden')
                } else {
                    $(this).addClass('hidden')
                }
            }
        });
    }
}