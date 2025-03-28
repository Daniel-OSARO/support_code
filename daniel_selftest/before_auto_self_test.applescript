tell application "iTerm"
    -- 새 창을 기본 프로필로 생성
    create window with default profile

    tell current session of current window
        -- 가로 분할 4개
        set newSession1 to split vertically with default profile
        set newSession2 to split vertically with default profile
        set newSession3 to split vertically with default profile
        set newSession4 to split vertically with default profile
        
        -- 각각 세로 분할
        tell newSession1
            split horizontally with default profile
        end tell
        tell newSession2
            split horizontally with default profile
        end tell
        tell newSession3
            split horizontally with default profile
        end tell
        tell newSession4
            split horizontally with default profile
        end tell
    end tell
    
    -- 현재 활성화된 창 닫기
    tell current session of current window
        close
    end tell

    -- Session command
    set servers to {"192.168.111.11", "192.168.111.15", "192.168.111.12", "192.168.111.16", "192.168.111.13", "192.168.111.15", "192.168.111.14", "check_version"}
    set i to 1
    repeat with aSession in sessions of current tab of current window
        tell aSession
            if i ≤ 7 then
                write text "ssh admin@" & item i of servers
                delay 0.1
                -- write text "docker ps"
            else -- check the versions
                write text "check_version"
            end if
            set i to i + 1
        end tell
    end repeat

    tell current window
        -- Access the third session (split pane)
        tell session 3 of current tab
            -- Send a command to the selected pane
            write text "ls -l"
        end tell
        tell session 6 of current tab
            -- Send a command to the selected pane
            write text "ls -l"
        end tell
    end tell

end tell
