"""
 Simplified datetime format string converter from LDML (Locale Data Markup
 Language) aka CLDR (Unicode Common Locale Data Repository) format to POSIX
 aka strftime format.

 Main usecase is using complete localization from CLDR with D3, which
 implements POSIX style of date formatting.

 References:
  - http://www.unicode.org/reports/tr35/tr35-dates.html#Date_Field_Symbol_Table
  - http://pubs.opengroup.org/onlinepubs/007908799/xsh/strftime.html
  - https://github.com/mbostock/d3/wiki/Time-Formatting#format
  - https://gist.github.com/saaj/0d6bb9b70964a1313cf5

 @license LGPLv2.1+
 @author maciekrb
"""

_PosixToLDMLMap = {
  '%Y'  : 'yyyy',  # year: 1985
  '%m'  : 'MM'  ,  # month: 09
  '%b'  : 'MMM' ,  # month: Sept
  '%B'  : 'MMMM',  # month: September
  '%e'  : 'd'   ,  # day: 3
  '%d'  : 'dd'  ,  # day: 03
  '%j'  : 'D'   ,  # day: 246
  '%A'  : 'EEEE',  # day: Tuesday
  '%I'  : 'hh'  ,  # 12-hour: 04
  '%H'  : 'HH'  ,  # 24-hour: 04
  '%M'  : 'mm'  ,  # minute: 09
  '%S'  : 'ss'  ,  # second: 06
  '%p'  : 'a'   ,  # AM or PM: AM
  '%Z'  : 'Z'      # timezone: -0400
}

def posix2LDML(date_fmt):
  for needle, rplcmt in _PosixToLDMLMap.items():
    date_fmt = date_fmt.replace(needle, rplcmt)
  return date_fmt
