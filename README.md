# arisa

환경 변수:
- `ARISA_BIND`: bind 주소, 기본값 `0.0.0.0:3000`
- `ARISA_UID`: Android user id
- `ARISA_CALLING_PKG`: Android 호출 package, 기본값 `com.android.shell`
- `ARISA_DB_PULL_DELAY`: DB 폴링 딜레이(ms), 기본값 `100`
- `ARISA_EXIT_ON_STDIN_CLOSE`: stdin이 닫힐 때 종료할지 여부, 기본값 `true`
## Python
```
uv add "git+https://github.com/ye-seola/arisa2.git#subdirectory=python" --branch main
```
