#!/bin/sh
xtrace_switch=$(set -o | grep xtrace | cut -f2 | grep on -c)
set +x
# 0. env
export LC_ALL=en_US.UTF-8
# java
export JAVA_HOME=/data/resys/var/jdk1.7.0_17
export CLASSPATH=.:$JAVA_HOME/lib:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
export PATH=$JAVA_HOME/bin:$PATH
# scala/sbt
export SCALA_HOME=/data/resys/var/scala-2.11.8
export PATH=/data/resys/bin:${SCALA_HOME}/bin:$PATH
# hadoop/yarn/lzo
export HADOOP_HOME="/usr/local/webserver/hadoop-resys"
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbi

n:$PATH
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export streaming_cmd="hadoop jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/data/resys/var/lzo-2.08/lib
# spark
export PATH=/data/resys/var/spark-2.0.2-bin-hadoop2.6/bin:$PATH
# hive
export PATH=/usr/local/webserver/hive-resys/bin/:$PATH

[[ $(uname -s) =~ "Linux" ]] && BASEDIR=$(dirname $(readlink -f "${BASH_SOURCE[0]}")) || BASEDIR=$(cd $(dirname "${BASH_SOURCE[0]}"); pwd -P)
function cover_me() {
	curl -s https://code.mesa.net/resys/GlobalCommonTools/raw/master/bin/argo_tool.sh > ${BASEDIR}/.tmp.sh
    diff ${BASEDIR}/.tmp.sh ${BASEDIR}/argo_tool.sh >/dev/null 2>&1 && {
        # same
        rm -f ${BASEDIR}/.tmp.sh
    } || {
        # diff
        mv -f ${BASEDIR}/.tmp.sh ${BASEDIR}/argo_tool.sh
        source ${BASEDIR}/argo_tool.sh
    }
    return 0
}
#cover_me
__tmpdir=$(mktemp -d /tmp/argo_tool.tmp.XXXXXXXXXX)
__today=$(date +%F)
[[ $BASH_VERSINFO -ge 4 ]] && {
declare -A __settings=(
    [exit_on_fail]=0
    [phones]=
    [emails]=test@126.com
    [job_timeout]=1800 # half an hour
    [job_timeout_times]=4 # 4 times, i.e. 2 hours
    [debug]=0 # debug mode
)
} || {
    __settings_exit_on_fail=0
    __settings_phones=
    __settings_emails=test@126.com
    __settings_job_timeout=1800 # half an hour
    __settings_job_timeout_times=4 # 4 times, i.e. 2 hours
    __settings_debug=0 #debug mode
}

# 1. basic functions
function log() { 
    prefix="[WARN][$(date +"%Y-%m-%d %H:%M:%S %:z")] $(caller):${FUNCNAME[1]}:${BASH_LINENO[0]}"
    index=1
    while read line; do
        echo "${prefix}-${index} ${line}" 
        ((index++))
    done <<< "$@"
}
function usage() { echo "Usage: ${FUNCNAME[1]} $@";}
function hget() { 
    [[ $# -eq 1 ]] && {
        [[ $BASH_VERSINFO -ge 4 ]] && echo ${__settings["$1"]} || { k="__settings_$1"; echo ${!k};}
    } || { usage key;};
}
function hset() { 
    [[ $# -eq 2 ]] && {
        [[ $BASH_VERSINFO -ge 4 ]] && __settings["$1"]="$2" || eval "__settings_$1=$2"
    } || { usage key value;};}
function get_ip() { /sbin/ifconfig | awk '$0~/inet /&&$0!~/127.0.0.1/{print $2}' | grep -o -E "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+";}
function quit() { 
    [[ $# -eq 1 ]] && { [[ $(hget exit_on_fail) -eq 1 ]] && {
        msg="$(log "${FUNCNAME[1]} exit $1")"
        send_mail "${FUNCNAME[1]}" "$msg"
        exit $1 
    } || return $1;} || { usage exit_code;};}
function caller() {
    local CALLER="${BASH_SOURCE[$((${#BASH_SOURCE[@]}-1))]}"
    [[ $(uname -s) =~ "Linux" ]] && {
        echo "$(readlink -f "$CALLER")"
    } || {
        local d=$(cd $(dirname "$CALLER"); pwd -P)
        local f=$(basename "$CALLER")
        echo "${d}/${f}"
    }
}
function hwc() {
    [[ -z "$1" ]] && { usage hdfs_path; echo 0; return 1;}
    exec 2>/dev/null
    hadoop fs -test -e $1/_COUNTER && {
        count=$(hadoop fs -cat $1/_COUNTER)
        [[ -n "$count" && $(echo "$count" | wc -l) -eq 1 ]] && {
            echo "$count"
            return 0
        }
    }
    output="/user/resys/tmp/hwc-$$-$RANDOM"
    hadoop fs -rm -r "${output}" >/dev/null
    ${streaming_cmd} -D mapreduce.job.name="${output##*/}@rec_algo_data" -input "$1" -output "${output}" -mapper "wc -l" >/dev/null
    ret=$?
    [[ ${ret} -eq 0 ]] && {
        hadoop fs -test -e ${output} && {
            num=$(hadoop fs -cat "${output}/part*" | awk 'BEGIN{s=0}{s+=$0}END{print s}')
            hadoop fs -rm -r "${output}" >/dev/null
            echo $num
        } || {
            msg="hwc count $1 error"
            log_msg="$(log "$msg")"
            echo "$log_msg"
            send_mail "${FUNCNAME[1]}-${__today}" "${log_msg}"
            echo 0
        }
    } || {
        msg="hwc count $1 error"
        log_msg="$(log "$msg")"
        echo "$log_msg"
        send_mail "${FUNCNAME[1]}-${__today}" "${log_msg}"
        echo 0
    }
    return 0
}
function hsetrep() {
    [[ -z "$1" ]] && { usage hdfs_path "replication[default 2]"; return 1;}
    hadoop fs -setrep "${2:-2}" "$1"
    return 0
}
function streaming() {
    hadoop fs -rm -r "$2"
    ${streaming_cmd} \
    -files "$3,$4" \
    -D stream.non.zero.exit.is.failure=false \
    -D mapreduce.job.name="$2@rec_algo_data" \
    -input "$1" \
    -output "$2" \
    -mapper "sh $3" \
    -reducer "sh $4"
    return 0
}
function hcompress() {
    [[ $# -lt 2 ]] && { usage input output "gzip/lzo/text[default gzip]" "replication[default 2]"; return 1;}
    hadoop fs -rm -r "$2"
    num=$(hadoop fs -ls "$1/part*" | wc -l)
    iszip=false
    zipclass=""
    case "${3:-gzip}" in
      gzip)
        iszip=true
        zipclass="org.apache.hadoop.io.compress.GzipCodec"
        ;;
      lzo)
        iszip=true
        zipclass="com.hadoop.compression.lzo.LzopCodec"
        ;;
      *)
        iszip=false
        zipclass="org.apache.hadoop.io.compress.GzipCodec"
        ;;
    esac
    ${streaming_cmd} \
    -D mapreduce.job.name="$2@rec_algo_data" \
    -D mapreduce.job.reduces=0 \
    -D dfs.replication="${4:-2}" \
    -D mapreduce.output.fileoutputformat.compress="$iszip" \
    -D mapreduce.output.fileoutputformat.compress.codec="$zipclass" \
    -input "$1" \
    -output "$2" \
    -mapper "cat"
    return 0
}
function send_mail() {
    [[ $(hget debug) -eq 1 ]] && return 0
    _tmp=$(hget emails)
    [[ $# -lt 2 || $# -gt 3 || ($# -eq 2 && -z "${_tmp}") ]] && {
        usage title body [receiver]
        return 1
    }
    local title="$1"
    local content="$2"
    local emails="${3:-${_tmp}}"
    content="$(echo "${content}" | sed -e 's#$#<br>#g')"
    /home/resys/bin/amail "spark-debug" "${emails}" "${title}" "<html>${content}</html>" > /dev/null 2>&1
    return $?
}
function send_sms() {
    [[ $(hget debug) -eq 1 ]] && return 0
    _tmp=$(hget phones)
    [[ $# -lt 1 || ($# -eq 1 && -z "${_tmp}") ]] && {
        usage message "[phone,phone...]"
        return 1
    }
    phones=${2:-${_tmp}}
    curl -s --data-urlencode sp=gd --data-urlencode number="$phones" --data-urlencode msg="【微店】$(get_ip)|$1" http://idc02-im-message-vip00/message/send >/dev/null 2>&1
    return $?
}
function install_hook() {
    cd ${BASEDIR}/../
    cat > .git/hooks/pre-commit <<'EOF'
#!/bin/sh
[[ $(uname -s) =~ "Linux" ]] && _tmp=$(dirname $(readlink -f "${BASH_SOURCE[0]}")) || _tmp=$(cd $(dirname "${BASH_SOURCE[0]}"); pwd -P)
GIT_DIR=$(cd ${_tmp}/../../.git; pwd -P)
curl -s https://code.mesa.net/resys/GlobalCommonTools/raw/master/bin/argo_tool.sh > bin/argo_tool.sh && sh bin/argo_tool.sh commit
EOF
    chmod +x .git/hooks/pre-commit
    return 0
}
function commit() {
    git_root=${GIT_DIR%.git}
    git_root=${git_root:-$(cd ${BASEDIR}/../; pwd)}
    cd ${git_root}
    abs_file="${BASEDIR}/$(basename "${BASH_SOURCE[0]}")"
    relative_file=${abs_file:${#git_root}}
    path_script=${relative_file#/}
    path_only="${path_script%/*}/"
    script=${relative_file##*/}
    output=$(git status --porcelain | grep -E "($path_script|$path_only)$")
    [[ -n "$output" ]] && git add "$path_script"
    return 0
}
function help() {
    index=1
    while read func; do
        help=$($func 2>/dev/null | grep -E "^Usage: .{1,}")
        [[ -n "$help" ]] && { 
            [[ $(uname -s) =~ "Linux" ]] && {
                echo -e "${index}. ${func}\n${help}\n"; ((index++));
            } || {
                echo "${index}. ${func}\n${help}\n"; ((index++));
            }
        } || { 
            [[ $(uname -s) =~ "Linux" ]] && {
                echo -e "${index}. ${func}\n"; ((index++));
            } || {
                echo "${index}. ${func}\n"; ((index++));
            }
        }
    done <<< "$(declare -F | awk '{print $3}' | grep -v -E "^(help|usage)$")"
}

# 2. job functions
function check_hadoop_wc() {
    [[ -z "$1" ]] && { usage hdfs_path "threshold[default 0.2]"; return 1;}
    local this_day="${1//\/\///}"
    hadoop fs -test -e "$this_day/_SUCCESS" || { log "ERROR: $this_day does not exist"; return 2;}
    dt=${this_day##*/}
    dir=${this_day%/*}

    last_day=$(hadoop fs -ls "${dir}" | awk '$0~/\//&&$NF~/[0-9-]+$/{print $NF}' | sort -nr | grep "${this_day}" -A1 | tail -n+2)
    [[ -z "$last_day" ]] && { log "ERROR: last day of $this_day does not exist"; return 3;}
    hadoop fs -test -e "$last_day/_SUCCESS" || { log "ERROR: $last_day does not exist"; return 4;}

    wc_this=$(hwc "${this_day}")
    wc_last=$(hwc "${last_day}")
    [[ $wc_this -eq 0 ]] && ((wc_this++))
    [[ $wc_last -eq 0 ]] && ((wc_last++))
    threshold=${2:-0.2}
    result=$(echo "a=$wc_last/$wc_this-1; a>$threshold || a<-$threshold" | bc -l)
    [[ $result -eq 1 ]] && {
        msg="wc ${wc_this}:${this_day} ${wc_last}:${last_day}. data is abnormal."
        log_msg="$(log "$msg")"
        echo "$log_msg"
        send_mail "${FUNCNAME[1]}-${__today}" "$log_msg"
        send_sms "$log_msg"
        # 已经报警，正常退出
        return 0
    }
    return 0
}
function check_dir_should_exist_success() {
    [[ -z "$1" ]] && { usage hdfs_path; return 1;}
    hadoop fs -test -e "$1/_SUCCESS"
    result=$?
    [[ $result -ne 0 ]] && {
        log "ERROR: check dir $1 does not exist"
        quit ${result}
        local temp_result=$?
        echo ${temp_result}
        return ${temp_result}
    }
    echo 0 && return 0
}
function check_and_wait_dir_should_exist_success() {
    [[ -z "$1" ]] && { usage hdfs_path "times[default 60]" "sleep_interval[default 30s]"; echo 1; return 1;}
    times=${2:-60}
    interval=${3:-30}
    SECONDS=0
    while ((times > 0)); do
        hadoop fs -test -e "$1/_SUCCESS" && { echo 0; return 0;}
        ((times--))
        sleep $interval
    done
    ((check_times = ${2:-60} - ${times}))    
    msg="ERROR: check dir $1 $check_times times, which costs $SECONDS, and all fails"
    log_msg="$(log "$msg")"
    echo "$log_msg"
    send_mail "${FUNCNAME[1]}-${__today}" "$log_msg"
    echo 1 && return 1
}
function check_dir_should_exist_success_and_updated() {
    [[ -z "$1" ]] && { usage hdfs_path; return 1;}
    check_dir_should_exist_success $1
    result=$?
    if [[ ${result} -eq 0 ]]; then
        update_date=$(hadoop fs -ls "$1/_SUCCESS" | awk '{print $6}')
        [ "${update_date}" == "${__today}" ] && { echo 0; return 0;} || { echo 1; return 1;}
    else
        { echo 1; return 1;}
    fi
}
function check_and_wait_dir_should_exist_success_and_updated() {
    [[ -z "$1" ]] && { usage hdfs_path "times[default 60]" "sleep_interval[default 30s]"; return 1;}
    times=${2:-60}
    interval=${3:-30}
    SECONDS=0
    while ((times > 0)); do
        hadoop fs -test -e "$1/_SUCCESS" && {
            update_date=$(hadoop fs -ls "$1/_SUCCESS" | awk '{print $6}')
            [ "${update_date}" == "${__today}" ] && echo 0 && return 0
        }
        ((times--))
        sleep $interval
    done
    ((check_times = ${2:-60} - ${times}))
    msg="ERROR: check dir $1 $check_times times, which costs $SECONDS, and all fails"
    log_msg="$(log "$msg")"
    echo "$log_msg"
    send_mail "${FUNCNAME[1]}-${__today}" "$log_msg"
    echo 1 && return 1
}
function remove_hadoop_dir_if_exist() {
    [[ -z "$1" ]] && { usage hdfs_path; return 1;}
    rnd=$RANDOM
    func="${FUNCNAME[1]}_${BASHPID}"
    echo "${rnd}-${func}" >> "${__tmpdir}/path.order"
    echo -e "${FUNCNAME[1]}\t$1" >> "${__tmpdir}/path.$func"
    hadoop fs -test -e $1 && hadoop fs -rm -r $1
    return 0
}
function get_recent_date_set() {
    [[ $# -lt 2 ]] && { usage start_date days "date_format[default %F]"; return 1;}
    format=${3:-%F}
    days_count=$(($2 - 1))
    dates=$(seq 0 ${days_count} | xargs -I{} date -d"$1 {} days ago" +"${format}" | tr '\n' ',' | sed -e 's/,$//g')
    echo "{$dates}"
    return 0
}
function clear_history() {
    [[ -z "$1" ]] && { usage hdfs_path "history_delete_interval[default 7]" "date_format[default %F]"; return 1;}
    history_delete_interval=${2:-7}
    format=${3:-%F}
    to_be_deleted="$1/$(date -d "${__today} ${history_delete_interval} days ago" +"${format}")"
    [[ -n "${to_be_deleted}" ]] && hadoop fs -test -e "${to_be_deleted}" && hadoop fs -rm -r "${to_be_deleted}"
    return 0
}
function keep_history() {
    [[ -z "$1" ]] && { usage hdfs_path "history_delete_interval[default 7]"; return 1;}
    history_delete_interval=${2:-7}
    to_be_deleted=$(hadoop fs -ls "$1" 2>/dev/null | awk '$0~/\//{print $NF}' | sort -r | tail -n+$(($history_delete_interval+1)) | head -n1)
    [[ -n "${to_be_deleted}" ]] && hadoop fs -test -e "${to_be_deleted}" && hadoop fs -rm -r "${to_be_deleted}"
    return 0
}
function get_ready_date() {
    [[ -z "$1" ]] && {
        usage hdfs_path [prefix]
        return 1; 
    }
    latest=$(hadoop fs -ls "${1}/${2}[0-9]*[0-9]/_SUCCESS" | awk '$0~/\//{split($NF,a,"/");print a[length(a)-1];}' | sort -r | head -n1)
    echo "${latest}"
    return 0
}
function get_ready_path() {
    [[ -z "$1" ]] && { 
        usage hdfs_path [prefix]
        return 1; 
    }
    latest=$(hadoop fs -ls "${1}/${2}[0-9]*[0-9]/_SUCCESS" | awk '$0~/\//{print $NF}' | sort -r | head -n1)
    echo "${latest%/_SUCCESS}"
    return 0
}
function wait_jobs() {
    local failed=0
    local wait_function=${FUNCNAME[1]}
    for pid in $(jobs -p); do
        log "wait for child $pid"
        wait $pid || ((failed++))
    done
    [[ $failed -eq 0 ]] && {
        log "all childs finished"
    } || {
        msg="${failed} child(s) of ${wait_function} failed"
        log_msg="$(log "$msg")"
        echo "$log_msg"
        send_mail "$FUNCNAME" "$log_msg"
        return $(quit $failed)
    }
    return 0
}
function spark_submit_graph() {
    uuid="${FUNCNAME[1]}-$RANDOM"
    check_job "$uuid" "--driver-java-options -Dbash.check=$uuid $@" &
    bg_pid=$!
    pkg_info="-Dbash.package=$(get_ip):${FUNCNAME[1]}:$(readlink -f ${BASH_SOURCE[1]})"
    spark-submit --driver-java-options "-Dbash.check=$uuid $pkg_info" "$@"
    status=$?
    kill -s 0 $bg_pid && kill -9 $bg_pid
    [[ $status -ne 0 ]] && {
        job_name=""
        job_name_start=0
        for x in "$@"; do
            ((job_name_start > 0)) && { job_name="$x"; break;}
            [[ "$x" == "--name" ]] && job_name_start=1
        done
        send_sms "FAILED-${job_name}$(log "FAILED $@")"
        send_mail "FAILED-${FUNCNAME[0]}" "$(log "FAILED $@")"
    } || check_output
    return $status
}
function check_job() {
    sleep 2s
    pid=$(ps aux | grep "$1" | grep -v -E "grep|$FUNCNAME" | awk '{print $2}')
    num=$(ps aux | grep "$1" | grep -v -E "grep|$FUNCNAME" | wc -l)
    [[ $num -ne 1 ]] && return 1
    shift
    job_timeout=$(hget job_timeout)
    job_times=$(hget job_timeout_times)
    SECONDS=0
    snapchat=$SECONDS
    success=0
    for ((i = 0; success < 1; i++)); do
        while ((success < 1 && SECONDS - snapchat < job_timeout)); do
            kill -s 0 $pid 2>/dev/null || { ((success++)); break;}
            sleep 1m
        done
        ((success > 0)) && break
        ((success < 1)) && kill -s 0 $pid 2>/dev/null && {
            log_msg="$(log "${FUNCNAME[2]} is still running after $(echo "scale=1;$SECONDS/3600" | bc) hours. $@")"
            echo "$log_msg"
            ((i >= job_times)) && send_sms "$log_msg"
            send_mail "${FUNCNAME[0]}-${__today}" "$log_msg"
        }
        snapchat=$SECONDS
    done
    return 0
}
function check_output() {
    return 0
    caller="-${FUNCNAME[2]}_$BASHPID"
    error_file="${__tmpdir}/path.${caller:1}.error"
    awk -F'-' '$0~/'$caller'$/{print $2}' "${__tmpdir}/path.order" | uniq | while read fun; do
        cat "${__tmpdir}/path.$fun" | while read file; do
            func=$(echo "$file" | cut -d$'\t' -f1)
            path=$(echo "$file" | cut -d$'\t' -f2)
            msg=$(check_hadoop_wc $path; echo $?;)
            code=$(echo "$msg" | tail -n-1)
            msg=$(echo "$msg" | head -n-1)
            [[ $code -ne 0 ]] && send_mail "${FUNCNAME[0]}-${__today}" "$msg [$func]"
            [[ $code -eq 2 ]] && echo "$file" >> "${error_file}"
        done
    done
    [[ -e "${error_file}" ]] && {
        num=$(wc -l "${error_file}" | awk '{print $1}')
        [[ $num -gt 0 ]] && {
            send_mail "${FUNCNAME[0]}-failed-jobs-${__today}" "$num failed files<br>$(cat "${error_file}" | sed -e 's/\n/<br>/g')"
        }
    }
    sed -i -e '/'$caller'$/d' "${__tmpdir}/path.order"
    rm -f "${__tmpdir}/path.${caller:1}"*
    return 0
}
function select_exists_input_path() {
    [[ $# -lt 1 ]] && { usage 'multiple_input_path delimiter[default comma(,)]'; return 1;}
    local input_path=$1
    local delimiter=${2:-,}
    local call_function=${FUNCNAME[1]}
    local valid_input_path=""
    local invalid_input_path=""
    while read line; do
        hadoop fs -ls ${line} > /dev/null && valid_input_path="${valid_input_path}${delimiter}${line}" || invalid_input_path="${invalid_input_path}${delimiter}${line}"
    done <<< "$(echo ${input_path} | sed -r "s/${delimiter}+/${delimiter}/g" | tr "${delimiter}" "\n")"
    
    valid_input_path=$(echo ${valid_input_path} | sed -r "s/^${delimiter}+//g" | sed -r "s/${delimiter}+$//g" | sed -r "s/${delimiter}+/${delimiter}/g")
    invalid_input_path=$(echo ${invalid_input_path} | sed -r "s/^${delimiter}+//g" | sed -r "s/${delimiter}+$//g" | sed -r "s/${delimiter}+/${delimiter}/g")
    [[ -n ${invalid_input_path} ]] && send_mail "Multiple-input-check-${__today}" "${invalid_input_path} not exist(s) in function ${call_function}"
    echo ${valid_input_path}
    return 0
}
function select_queue() {
    local cluster_url="http://resys-yarn.bigdata.mesa.net/cluster/scheduler"
    local awk_lines=$(cat <<'EOF'
BEGIN {
    queue_black_list["image"]
    queue_black_list["push"]
}
/Queue Status/ {
    match($0, "'(.*)'", queue)
    queue_name = queue[1]
}
/Absolute Used Capacity/ {
    getline
    getline
    getline
    match($0,"([^ ]*)%$", use)
    abs_used_cap = use[1]
}
/Absolute Max Capacity/ {
    getline
    getline
    getline
    match($0,"([^ ]*)%$", max)
    abs_max_cap = max[1]
    queue_remains = (100 - abs_used_cap) * abs_max_cap
    dic[queue_name] = queue_remains
}
END {
    queue_name = ""
    max_remain = 0
    for (i in dic) {
        if (dic[i] > max_remain && !(i in queue_black_list)) {
            max_remain = dic[i]
            queue_name = i
        }
    }
    if (length(queue_name) == 0) { print "default" } else { print queue_name }
}
EOF
)
    curl -s ${cluster_url} | gawk --source "${awk_lines}"
}

[[ $xtrace_switch -eq 1 ]] && set -x || set +x
[[ ${#BASH_SOURCE[@]} -eq 1 ]] && "$@"
