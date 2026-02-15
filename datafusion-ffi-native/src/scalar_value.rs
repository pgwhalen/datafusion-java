//! FFI_ScalarValue — C-style tagged union for scalar values.

use datafusion::common::ScalarValue;
use std::ffi::c_char;

use crate::error::{clear_error, set_error_return};

// ============================================================
// FFI_ScalarValue — C-style tagged union
// ============================================================

/// Data union for FFI_ScalarValue. 32 bytes to accommodate Decimal256.
#[repr(C)]
pub union ScalarValueData {
    pub bool_val: i32,
    pub i64_val: i64,
    pub u64_val: u64,
    pub f64_val: f64,
    pub f16_val: u16,
    pub interval_dt: [i32; 2],
    pub interval_mdn: IntervalMDN,
    pub i128_val: [i64; 2],
    pub bytes32: [u8; 32],
}

/// Month-Day-Nano interval components.
#[repr(C)]
#[derive(Copy, Clone)]
pub struct IntervalMDN {
    pub months: i32,
    pub days: i32,
    pub nanos: i64,
}

/// C-compatible representation of a DataFusion ScalarValue.
#[repr(C)]
pub struct FFI_ScalarValue {
    pub type_tag: i32,
    pub is_null: i32,
    pub data: ScalarValueData,
    pub data_ptr: *const u8,
    pub data_len: usize,
    pub precision: u8,
    pub scale: i8,
    pub _padding: [u8; 2],
    pub fixed_size: i32,
}

// Type tag constants — must match Java's ScalarValue.TAG_* constants
const TAG_NULL: i32 = 0;
const TAG_BOOLEAN: i32 = 1;
const TAG_INT8: i32 = 2;
const TAG_INT16: i32 = 3;
const TAG_INT32: i32 = 4;
const TAG_INT64: i32 = 5;
const TAG_UINT8: i32 = 6;
const TAG_UINT16: i32 = 7;
const TAG_UINT32: i32 = 8;
const TAG_UINT64: i32 = 9;
const TAG_FLOAT16: i32 = 10;
const TAG_FLOAT32: i32 = 11;
const TAG_FLOAT64: i32 = 12;
const TAG_UTF8: i32 = 13;
const TAG_LARGE_UTF8: i32 = 14;
const TAG_UTF8_VIEW: i32 = 15;
const TAG_BINARY: i32 = 16;
const TAG_LARGE_BINARY: i32 = 17;
const TAG_BINARY_VIEW: i32 = 18;
const TAG_FIXED_SIZE_BINARY: i32 = 19;
const TAG_DATE32: i32 = 20;
const TAG_DATE64: i32 = 21;
const TAG_TIME32_SECOND: i32 = 22;
const TAG_TIME32_MILLISECOND: i32 = 23;
const TAG_TIME64_MICROSECOND: i32 = 24;
const TAG_TIME64_NANOSECOND: i32 = 25;
const TAG_TIMESTAMP_SECOND: i32 = 26;
const TAG_TIMESTAMP_MILLISECOND: i32 = 27;
const TAG_TIMESTAMP_MICROSECOND: i32 = 28;
const TAG_TIMESTAMP_NANOSECOND: i32 = 29;
const TAG_DURATION_SECOND: i32 = 30;
const TAG_DURATION_MILLISECOND: i32 = 31;
const TAG_DURATION_MICROSECOND: i32 = 32;
const TAG_DURATION_NANOSECOND: i32 = 33;
const TAG_INTERVAL_YEAR_MONTH: i32 = 34;
const TAG_INTERVAL_DAY_TIME: i32 = 35;
const TAG_INTERVAL_MONTH_DAY_NANO: i32 = 36;
const TAG_DECIMAL32: i32 = 37;
const TAG_DECIMAL64: i32 = 38;
const TAG_DECIMAL128: i32 = 39;
const TAG_DECIMAL256: i32 = 40;

/// Maps a Rust ScalarValue to the FFI struct. String/binary data pointers are borrowed
/// from the ScalarValue and valid as long as the GuaranteesHandle lives.
pub(crate) fn scalar_to_ffi(value: &ScalarValue, out: &mut FFI_ScalarValue) {
    // Zero the struct first
    out.data = ScalarValueData { bytes32: [0; 32] };
    out.data_ptr = std::ptr::null();
    out.data_len = 0;
    out.precision = 0;
    out.scale = 0;
    out._padding = [0; 2];
    out.fixed_size = 0;
    out.is_null = 0;

    match value {
        ScalarValue::Null => {
            out.type_tag = TAG_NULL;
            out.is_null = 1;
        }
        ScalarValue::Boolean(v) => {
            out.type_tag = TAG_BOOLEAN;
            match v {
                Some(b) => out.data.bool_val = if *b { 1 } else { 0 },
                None => out.is_null = 1,
            }
        }
        ScalarValue::Int8(v) => {
            out.type_tag = TAG_INT8;
            match v {
                Some(i) => out.data.i64_val = *i as i64,
                None => out.is_null = 1,
            }
        }
        ScalarValue::Int16(v) => {
            out.type_tag = TAG_INT16;
            match v {
                Some(i) => out.data.i64_val = *i as i64,
                None => out.is_null = 1,
            }
        }
        ScalarValue::Int32(v) => {
            out.type_tag = TAG_INT32;
            match v {
                Some(i) => out.data.i64_val = *i as i64,
                None => out.is_null = 1,
            }
        }
        ScalarValue::Int64(v) => {
            out.type_tag = TAG_INT64;
            match v {
                Some(i) => out.data.i64_val = *i,
                None => out.is_null = 1,
            }
        }
        ScalarValue::UInt8(v) => {
            out.type_tag = TAG_UINT8;
            match v {
                Some(i) => out.data.u64_val = *i as u64,
                None => out.is_null = 1,
            }
        }
        ScalarValue::UInt16(v) => {
            out.type_tag = TAG_UINT16;
            match v {
                Some(i) => out.data.u64_val = *i as u64,
                None => out.is_null = 1,
            }
        }
        ScalarValue::UInt32(v) => {
            out.type_tag = TAG_UINT32;
            match v {
                Some(i) => out.data.u64_val = *i as u64,
                None => out.is_null = 1,
            }
        }
        ScalarValue::UInt64(v) => {
            out.type_tag = TAG_UINT64;
            match v {
                Some(i) => out.data.u64_val = *i,
                None => out.is_null = 1,
            }
        }
        ScalarValue::Float16(v) => {
            out.type_tag = TAG_FLOAT16;
            match v {
                Some(f) => out.data.f16_val = f.to_bits(),
                None => out.is_null = 1,
            }
        }
        ScalarValue::Float32(v) => {
            out.type_tag = TAG_FLOAT32;
            match v {
                Some(f) => out.data.f64_val = *f as f64,
                None => out.is_null = 1,
            }
        }
        ScalarValue::Float64(v) => {
            out.type_tag = TAG_FLOAT64;
            match v {
                Some(f) => out.data.f64_val = *f,
                None => out.is_null = 1,
            }
        }
        ScalarValue::Utf8(v) => {
            out.type_tag = TAG_UTF8;
            match v {
                Some(s) => {
                    out.data_ptr = s.as_ptr();
                    out.data_len = s.len();
                }
                None => out.is_null = 1,
            }
        }
        ScalarValue::LargeUtf8(v) => {
            out.type_tag = TAG_LARGE_UTF8;
            match v {
                Some(s) => {
                    out.data_ptr = s.as_ptr();
                    out.data_len = s.len();
                }
                None => out.is_null = 1,
            }
        }
        ScalarValue::Utf8View(v) => {
            out.type_tag = TAG_UTF8_VIEW;
            match v {
                Some(s) => {
                    out.data_ptr = s.as_ptr();
                    out.data_len = s.len();
                }
                None => out.is_null = 1,
            }
        }
        ScalarValue::Binary(v) => {
            out.type_tag = TAG_BINARY;
            match v {
                Some(b) => {
                    out.data_ptr = b.as_ptr();
                    out.data_len = b.len();
                }
                None => out.is_null = 1,
            }
        }
        ScalarValue::LargeBinary(v) => {
            out.type_tag = TAG_LARGE_BINARY;
            match v {
                Some(b) => {
                    out.data_ptr = b.as_ptr();
                    out.data_len = b.len();
                }
                None => out.is_null = 1,
            }
        }
        ScalarValue::BinaryView(v) => {
            out.type_tag = TAG_BINARY_VIEW;
            match v {
                Some(b) => {
                    out.data_ptr = b.as_ptr();
                    out.data_len = b.len();
                }
                None => out.is_null = 1,
            }
        }
        ScalarValue::FixedSizeBinary(size, v) => {
            out.type_tag = TAG_FIXED_SIZE_BINARY;
            out.fixed_size = *size;
            match v {
                Some(b) => {
                    out.data_ptr = b.as_ptr();
                    out.data_len = b.len();
                }
                None => out.is_null = 1,
            }
        }
        ScalarValue::Date32(v) => {
            out.type_tag = TAG_DATE32;
            match v {
                Some(d) => out.data.i64_val = *d as i64,
                None => out.is_null = 1,
            }
        }
        ScalarValue::Date64(v) => {
            out.type_tag = TAG_DATE64;
            match v {
                Some(d) => out.data.i64_val = *d,
                None => out.is_null = 1,
            }
        }
        ScalarValue::Time32Second(v) => {
            out.type_tag = TAG_TIME32_SECOND;
            match v {
                Some(t) => out.data.i64_val = *t as i64,
                None => out.is_null = 1,
            }
        }
        ScalarValue::Time32Millisecond(v) => {
            out.type_tag = TAG_TIME32_MILLISECOND;
            match v {
                Some(t) => out.data.i64_val = *t as i64,
                None => out.is_null = 1,
            }
        }
        ScalarValue::Time64Microsecond(v) => {
            out.type_tag = TAG_TIME64_MICROSECOND;
            match v {
                Some(t) => out.data.i64_val = *t,
                None => out.is_null = 1,
            }
        }
        ScalarValue::Time64Nanosecond(v) => {
            out.type_tag = TAG_TIME64_NANOSECOND;
            match v {
                Some(t) => out.data.i64_val = *t,
                None => out.is_null = 1,
            }
        }
        ScalarValue::TimestampSecond(v, tz) => {
            out.type_tag = TAG_TIMESTAMP_SECOND;
            match v {
                Some(t) => {
                    out.data.i64_val = *t;
                    if let Some(tz_str) = tz {
                        let tz_s = tz_str.to_string();
                        out.data_ptr = tz_s.as_ptr();
                        out.data_len = tz_s.len();
                        std::mem::forget(tz_s); // Leaked — but GuaranteesHandle owns the LiteralGuarantee
                        // Actually, the tz Arc<str> is borrowed from the ScalarValue which lives
                        // in the GuaranteesHandle. We need to be more careful here.
                    }
                }
                None => out.is_null = 1,
            }
        }
        ScalarValue::TimestampMillisecond(v, tz) => {
            out.type_tag = TAG_TIMESTAMP_MILLISECOND;
            match v {
                Some(t) => {
                    out.data.i64_val = *t;
                    if let Some(tz_str) = tz {
                        let tz_s = tz_str.to_string();
                        out.data_ptr = tz_s.as_ptr();
                        out.data_len = tz_s.len();
                        std::mem::forget(tz_s);
                    }
                }
                None => out.is_null = 1,
            }
        }
        ScalarValue::TimestampMicrosecond(v, tz) => {
            out.type_tag = TAG_TIMESTAMP_MICROSECOND;
            match v {
                Some(t) => {
                    out.data.i64_val = *t;
                    if let Some(tz_str) = tz {
                        let tz_s = tz_str.to_string();
                        out.data_ptr = tz_s.as_ptr();
                        out.data_len = tz_s.len();
                        std::mem::forget(tz_s);
                    }
                }
                None => out.is_null = 1,
            }
        }
        ScalarValue::TimestampNanosecond(v, tz) => {
            out.type_tag = TAG_TIMESTAMP_NANOSECOND;
            match v {
                Some(t) => {
                    out.data.i64_val = *t;
                    if let Some(tz_str) = tz {
                        let tz_s = tz_str.to_string();
                        out.data_ptr = tz_s.as_ptr();
                        out.data_len = tz_s.len();
                        std::mem::forget(tz_s);
                    }
                }
                None => out.is_null = 1,
            }
        }
        ScalarValue::DurationSecond(v) => {
            out.type_tag = TAG_DURATION_SECOND;
            match v {
                Some(d) => out.data.i64_val = *d,
                None => out.is_null = 1,
            }
        }
        ScalarValue::DurationMillisecond(v) => {
            out.type_tag = TAG_DURATION_MILLISECOND;
            match v {
                Some(d) => out.data.i64_val = *d,
                None => out.is_null = 1,
            }
        }
        ScalarValue::DurationMicrosecond(v) => {
            out.type_tag = TAG_DURATION_MICROSECOND;
            match v {
                Some(d) => out.data.i64_val = *d,
                None => out.is_null = 1,
            }
        }
        ScalarValue::DurationNanosecond(v) => {
            out.type_tag = TAG_DURATION_NANOSECOND;
            match v {
                Some(d) => out.data.i64_val = *d,
                None => out.is_null = 1,
            }
        }
        ScalarValue::IntervalYearMonth(v) => {
            out.type_tag = TAG_INTERVAL_YEAR_MONTH;
            match v {
                Some(i) => out.data.i64_val = *i as i64,
                None => out.is_null = 1,
            }
        }
        ScalarValue::IntervalDayTime(v) => {
            out.type_tag = TAG_INTERVAL_DAY_TIME;
            match v {
                Some(i) => {
                    // DayTime is stored as i64: upper 32 bits = days, lower 32 bits = ms
                    let days = (i.days) as i32;
                    let millis = (i.milliseconds) as i32;
                    out.data.interval_dt = [days, millis];
                }
                None => out.is_null = 1,
            }
        }
        ScalarValue::IntervalMonthDayNano(v) => {
            out.type_tag = TAG_INTERVAL_MONTH_DAY_NANO;
            match v {
                Some(i) => {
                    out.data.interval_mdn = IntervalMDN {
                        months: i.months,
                        days: i.days,
                        nanos: i.nanoseconds,
                    };
                }
                None => out.is_null = 1,
            }
        }
        ScalarValue::Decimal32(v, precision, scale) => {
            out.type_tag = TAG_DECIMAL32;
            out.precision = *precision;
            out.scale = *scale;
            match v {
                Some(d) => out.data.i64_val = *d as i64,
                None => out.is_null = 1,
            }
        }
        ScalarValue::Decimal64(v, precision, scale) => {
            out.type_tag = TAG_DECIMAL64;
            out.precision = *precision;
            out.scale = *scale;
            match v {
                Some(d) => out.data.i64_val = *d,
                None => out.is_null = 1,
            }
        }
        ScalarValue::Decimal128(v, precision, scale) => {
            out.type_tag = TAG_DECIMAL128;
            out.precision = *precision;
            out.scale = *scale;
            match v {
                Some(d) => {
                    let bytes = d.to_le_bytes();
                    let low = i64::from_le_bytes(bytes[0..8].try_into().unwrap());
                    let high = i64::from_le_bytes(bytes[8..16].try_into().unwrap());
                    out.data.i128_val = [low, high];
                }
                None => out.is_null = 1,
            }
        }
        ScalarValue::Decimal256(v, precision, scale) => {
            out.type_tag = TAG_DECIMAL256;
            out.precision = *precision;
            out.scale = *scale;
            match v {
                Some(d) => {
                    let bytes = d.to_le_bytes();
                    let mut out_bytes = [0u8; 32];
                    out_bytes.copy_from_slice(&bytes);
                    out.data.bytes32 = out_bytes;
                }
                None => out.is_null = 1,
            }
        }
        // Complex types — not supported in FFI
        _ => {
            out.type_tag = TAG_NULL;
            out.is_null = 1;
        }
    }
}

/// Returns the size of FFI_ScalarValue for Java-side validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_scalar_value_size() -> usize {
    std::mem::size_of::<FFI_ScalarValue>()
}

/// Test helper: creates a predefined ScalarValue for the given type tag and writes it
/// to the FFI struct. Used by ScalarValueTest.java to validate round-trip serialization.
///
/// # Safety
/// - `scalar_out` must point to an FFI_ScalarValue-sized allocation
#[no_mangle]
pub unsafe extern "C" fn datafusion_test_scalar_value(
    type_tag: i32,
    scalar_out: *mut FFI_ScalarValue,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if scalar_out.is_null() {
        return set_error_return(error_out, "scalar_out is null");
    }

    let value = match type_tag {
        TAG_NULL => ScalarValue::Null,
        TAG_BOOLEAN => ScalarValue::Boolean(Some(true)),
        TAG_INT8 => ScalarValue::Int8(Some(42)),
        TAG_INT16 => ScalarValue::Int16(Some(1234)),
        TAG_INT32 => ScalarValue::Int32(Some(123456)),
        TAG_INT64 => ScalarValue::Int64(Some(1234567890)),
        TAG_UINT8 => ScalarValue::UInt8(Some(200)),
        TAG_UINT16 => ScalarValue::UInt16(Some(60000)),
        TAG_UINT32 => ScalarValue::UInt32(Some(3000000000)),
        TAG_UINT64 => ScalarValue::UInt64(Some(18000000000000000000)),
        TAG_FLOAT16 => ScalarValue::Float16(Some(half::f16::from_f32(3.14))),
        TAG_FLOAT32 => ScalarValue::Float32(Some(3.14)),
        TAG_FLOAT64 => ScalarValue::Float64(Some(2.718281828)),
        TAG_UTF8 => ScalarValue::Utf8(Some("hello".to_string())),
        TAG_LARGE_UTF8 => ScalarValue::LargeUtf8(Some("large_hello".to_string())),
        TAG_UTF8_VIEW => ScalarValue::Utf8View(Some("view_hello".to_string())),
        TAG_BINARY => ScalarValue::Binary(Some(vec![0xDE, 0xAD, 0xBE, 0xEF])),
        TAG_LARGE_BINARY => ScalarValue::LargeBinary(Some(vec![0xCA, 0xFE])),
        TAG_BINARY_VIEW => ScalarValue::BinaryView(Some(vec![0xBA, 0xBE])),
        TAG_FIXED_SIZE_BINARY => ScalarValue::FixedSizeBinary(4, Some(vec![0x01, 0x02, 0x03, 0x04])),
        TAG_DATE32 => ScalarValue::Date32(Some(19000)),
        TAG_DATE64 => ScalarValue::Date64(Some(1640995200000)),
        TAG_TIME32_SECOND => ScalarValue::Time32Second(Some(3600)),
        TAG_TIME32_MILLISECOND => ScalarValue::Time32Millisecond(Some(3600000)),
        TAG_TIME64_MICROSECOND => ScalarValue::Time64Microsecond(Some(3600000000)),
        TAG_TIME64_NANOSECOND => ScalarValue::Time64Nanosecond(Some(3600000000000)),
        TAG_TIMESTAMP_SECOND => ScalarValue::TimestampSecond(Some(1640995200), None),
        TAG_TIMESTAMP_MILLISECOND => ScalarValue::TimestampMillisecond(Some(1640995200000), None),
        TAG_TIMESTAMP_MICROSECOND => ScalarValue::TimestampMicrosecond(Some(1640995200000000), None),
        TAG_TIMESTAMP_NANOSECOND => ScalarValue::TimestampNanosecond(Some(1640995200000000000), None),
        TAG_DURATION_SECOND => ScalarValue::DurationSecond(Some(3600)),
        TAG_DURATION_MILLISECOND => ScalarValue::DurationMillisecond(Some(3600000)),
        TAG_DURATION_MICROSECOND => ScalarValue::DurationMicrosecond(Some(3600000000)),
        TAG_DURATION_NANOSECOND => ScalarValue::DurationNanosecond(Some(3600000000000)),
        TAG_INTERVAL_YEAR_MONTH => ScalarValue::IntervalYearMonth(Some(14)),
        TAG_INTERVAL_DAY_TIME => {
            use arrow::datatypes::IntervalDayTime;
            ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(5, 3600000)))
        }
        TAG_INTERVAL_MONTH_DAY_NANO => {
            use arrow::datatypes::IntervalMonthDayNano;
            ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(2, 10, 500000000)))
        }
        TAG_DECIMAL32 => ScalarValue::Decimal32(Some(12345), 7, 2),
        TAG_DECIMAL64 => ScalarValue::Decimal64(Some(123456789), 12, 3),
        TAG_DECIMAL128 => ScalarValue::Decimal128(Some(1234567890123456789), 20, 5),
        TAG_DECIMAL256 => {
            use arrow::datatypes::i256;
            ScalarValue::Decimal256(Some(i256::from_i128(9876543210123456789)), 30, 10)
        }
        _ => {
            return set_error_return(error_out, &format!("Unsupported type tag for test: {}", type_tag));
        }
    };

    // For string/binary types, the test value must be kept alive
    // We need to store the value and pass borrowed pointers
    // Box it so the pointer remains stable
    let boxed = Box::new(value);
    scalar_to_ffi(&boxed, &mut *scalar_out);
    // Leak the boxed value - it will never be freed, but this is a test helper
    let _ = Box::into_raw(boxed);

    0
}
