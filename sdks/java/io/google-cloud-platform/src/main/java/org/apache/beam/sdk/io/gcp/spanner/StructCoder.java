package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class StructCoder extends AtomicCoder<Struct> {
    @Override
    public void encode(Struct value, OutputStream outStream) throws IOException {
        ObjectOutputStream out = new ObjectOutputStream(outStream);
        encodeInternal(value, out);
    }

    @Override
    public Struct decode(InputStream inStream) throws IOException {
        try {
            ObjectInputStream in = new ObjectInputStream(inStream);
            return decodeInternal(in);
        } catch (ClassNotFoundException e) {
            throw new CoderException(e);
        }
    }

    private void encodeInternal(Struct value, ObjectOutputStream out) throws IOException {
        Type type = value.getType();
        out.writeObject(type);
        List<Type.StructField> fields = type.getStructFields();
        for (int i = 0; i < fields.size(); i++) {
            Type fieldType = fields.get(i).getType();
            if (value.isNull(i)) {
                out.writeBoolean(true);
                continue;
            }
            out.writeBoolean(false);
            switch (fieldType.getCode()) {
                case BOOL:
                    out.writeBoolean(value.getBoolean(i));
                    break;
                case INT64:
                    out.writeLong(value.getLong(i));
                    break;
                case FLOAT64:
                    out.writeDouble(value.getDouble(i));
                    break;
                case STRING:
                    out.writeUTF(value.getString(i));
                    break;
                case BYTES:
                    out.writeObject(value.getBytes(i));
                    break;
                case TIMESTAMP:
                    out.writeObject(value.getTimestamp(i));
                    break;
                case DATE:
                    out.writeObject(value.getDate(i));
                    break;
                case ARRAY:
                    Type arrayElementType = fieldType.getArrayElementType();
                    switch (arrayElementType.getCode()) {
                        case BOOL: {
                            List<Boolean> boolArray = value.getBooleanList(i);
                            out.writeInt(boolArray.size());
                            for (Boolean val : boolArray) {
                                if (val == null) {
                                    out.writeBoolean(true);
                                } else {
                                    out.writeBoolean(false);
                                    out.writeBoolean(val);
                                }
                            }
                            break;
                        }
                        case INT64: {
                            List<Long> array = value.getLongList(i);
                            out.writeInt(array.size());
                            for (Long val : array) {
                                if (val == null) {
                                    out.writeBoolean(true);
                                } else {
                                    out.writeBoolean(false);
                                    out.writeLong(val);
                                }
                            }
                            break;
                        }
                        case FLOAT64: {
                            List<Double> array = value.getDoubleList(i);
                            out.writeInt(array.size());
                            for (Double val : array) {
                                if (val == null) {
                                    out.writeBoolean(true);
                                } else {
                                    out.writeBoolean(false);
                                    out.writeDouble(val);
                                }
                            }
                            break;
                        }
                        case STRING: {
                            List<String> array = value.getStringList(i);
                            out.writeInt(array.size());
                            for (String val : array) {
                                if (val == null) {
                                    out.writeBoolean(true);
                                } else {
                                    out.writeBoolean(false);
                                    out.writeUTF(val);
                                }
                            }
                            break;
                        }
                        case BYTES: {
                            List<ByteArray> array = value.getBytesList(i);
                            out.writeInt(array.size());
                            for (ByteArray val : array) {
                                if (val == null) {
                                    out.writeBoolean(true);
                                } else {
                                    out.writeBoolean(false);
                                    out.writeObject(val);
                                }
                            }
                            break;
                        }
                        case TIMESTAMP: {
                            List<Timestamp> array = value.getTimestampList(i);
                            out.writeInt(array.size());
                            for (Timestamp val : array) {
                                if (val == null) {
                                    out.writeBoolean(true);
                                } else {
                                    out.writeBoolean(false);
                                    out.writeObject(val);
                                }
                            }
                            break;
                        }
                        case DATE: {
                            List<Date> array = value.getDateList(i);
                            out.writeInt(array.size());
                            for (Date val : array) {
                                if (val == null) {
                                    out.writeBoolean(true);
                                } else {
                                    out.writeBoolean(false);
                                    out.writeObject(val);
                                }
                            }
                            break;
                        }
                        case STRUCT: {
                            List<Struct> array = value.getStructList(i);
                            int size = array.size();
                            out.writeInt(size);
                            for (int j = 0; j < size; j++) {
                                Struct child = array.get(j);
                                if (child == null) {
                                    out.writeBoolean(true);
                                } else {
                                    out.writeBoolean(false);
                                    encodeInternal(child, out);
                                }
                            }
                            break;
                        }
                        default:
                            throw new AssertionError("Invalid type " + arrayElementType);
                    }
                    break;
                default:
                    throw new AssertionError("Invalid type " + fieldType);
            }
        }
        out.flush();
    }

    private Struct decodeInternal(ObjectInputStream in) throws IOException, ClassNotFoundException {
        Type type = (Type) in.readObject();
        Struct.Builder builder = Struct.newBuilder();
        for (Type.StructField field : type.getStructFields()) {
            Type fieldType = field.getType();
            String fieldName = field.getName();
            switch (fieldType.getCode()) {
                case BOOL: {
                    boolean isNull = in.readBoolean();
                    if (isNull) {
                        builder.set(fieldName).to((Boolean) null);
                    } else {
                        builder.set(fieldName).to(in.readBoolean());
                    }
                    break;
                }
                case INT64: {
                    boolean isNull = in.readBoolean();
                    if (isNull) {
                        builder.set(fieldName).to((Long) null);
                    } else {
                        builder.set(fieldName).to(in.readLong());
                    }
                    break;
                }
                case FLOAT64: {
                    boolean isNull = in.readBoolean();
                    if (isNull) {
                        builder.set(fieldName).to((Double) null);
                    } else {
                        builder.set(fieldName).to(in.readDouble());
                    }
                    break;
                }
                case STRING: {
                    boolean isNull = in.readBoolean();
                    if (isNull) {
                        builder.set(fieldName).to((String) null);
                    } else {
                        builder.set(fieldName).to(in.readUTF());
                    }
                    break;
                }
                case BYTES: {
                    boolean isNull = in.readBoolean();
                    if (isNull) {
                        builder.set(fieldName).to((ByteArray) null);
                    } else {
                        builder.set(fieldName).to((ByteArray) in.readObject());
                    }
                    break;
                }
                case TIMESTAMP: {
                    boolean isNull = in.readBoolean();
                    if (isNull) {
                        builder.set(fieldName).to((Timestamp) null);
                    } else {
                        builder.set(fieldName).to((Timestamp) in.readObject());
                    }
                    break;
                }
                case DATE: {
                    boolean isNull = in.readBoolean();
                    if (isNull) {
                        builder.set(fieldName).to((Date) null);
                    } else {
                        builder.set(fieldName).to((Date) in.readObject());
                    }
                    break;
                }
                case ARRAY:
                    Type arrayType = fieldType.getArrayElementType();
                    switch (arrayType.getCode()) {
                        case BOOL: {
                            List<Boolean> array = null;
                            boolean isNull = in.readBoolean();
                            if (!isNull) {
                                int size = in.readInt();
                                array = new ArrayList<>(size);
                                for (int j = 0; j < size; j++) {
                                    isNull = in.readBoolean();
                                    if (isNull) {
                                        array.add(null);
                                    } else {
                                        array.add(in.readBoolean());
                                    }
                                }
                            }
                            builder.set(fieldName).toBoolArray(array);
                            break;
                        }
                        case INT64: {
                            List<Long> array = null;
                            boolean isNull = in.readBoolean();
                            if (!isNull) {
                                int size = in.readInt();
                                array = new ArrayList<>(size);
                                for (int j = 0; j < size; j++) {
                                    isNull = in.readBoolean();
                                    if (isNull) {
                                        array.add(null);
                                    } else {
                                        array.add(in.readLong());
                                    }
                                }
                            }
                            builder.set(fieldName).toInt64Array(array);
                            break;
                        }
                        case FLOAT64: {
                            List<Double> array = null;
                            boolean isNull = in.readBoolean();
                            if (!isNull) {
                                int size = in.readInt();
                                array = new ArrayList<>(size);
                                for (int j = 0; j < size; j++) {
                                    isNull = in.readBoolean();
                                    if (isNull) {
                                        array.add(null);
                                    } else {
                                        array.add(in.readDouble());
                                    }
                                }
                            }
                            builder.set(fieldName).toFloat64Array(array);
                            break;
                        }
                        case STRING: {
                            List<String> array = null;
                            boolean isNull = in.readBoolean();
                            if (!isNull) {
                                int size = in.readInt();
                                array = new ArrayList<>(size);
                                for (int j = 0; j < size; j++) {
                                    isNull = in.readBoolean();
                                    if (isNull) {
                                        array.add(null);
                                    } else {
                                        array.add(in.readUTF());
                                    }
                                }
                            }
                            builder.set(fieldName).toStringArray(array);
                            break;
                        }
                        case BYTES: {
                            List<ByteArray> array = null;
                            boolean isNull = in.readBoolean();
                            if (!isNull) {
                                int size = in.readInt();
                                array = new ArrayList<>(size);
                                for (int j = 0; j < size; j++) {
                                    isNull = in.readBoolean();
                                    if (isNull) {
                                        array.add(null);
                                    } else {
                                        array.add((ByteArray) in.readObject());
                                    }
                                }
                            }
                            builder.set(fieldName).toBytesArray(array);
                            break;
                        }
                        case TIMESTAMP: {
                            List<Timestamp> array = null;
                            boolean isNull = in.readBoolean();
                            if (!isNull) {
                                int size = in.readInt();
                                array = new ArrayList<>(size);
                                for (int j = 0; j < size; j++) {
                                    isNull = in.readBoolean();
                                    if (isNull) {
                                        array.add(null);
                                    } else {
                                        array.add((Timestamp) in.readObject());
                                    }
                                }
                            }
                            builder.set(fieldName).toTimestampArray(array);
                            break;
                        }
                        case DATE: {
                            List<Date> array = null;
                            boolean isNull = in.readBoolean();
                            if (!isNull) {
                                int size = in.readInt();
                                array = new ArrayList<>(size);
                                for (int j = 0; j < size; j++) {
                                    isNull = in.readBoolean();
                                    if (isNull) {
                                        array.add(null);
                                    } else {
                                        array.add((Date) in.readObject());
                                    }
                                }
                            }
                            builder.set(fieldName).toDateArray(array);
                            break;
                        }
                        case STRUCT: {
                            List<Struct> children;
                            boolean isNull = in.readBoolean();
                            if (!isNull) {
                                int size = in.readInt();
                                children = new ArrayList<>(size);
                                for (int j = 0; j < size; j++) {
                                    isNull = in.readBoolean();
                                    if (isNull) {
                                        children.add(null);
                                    } else {
                                        Struct child = decodeInternal(in);
                                        children.add(child);
                                    }
                                }
                                builder.add(fieldName, arrayType.getStructFields(), children);
                            }
                            break;
                        }
                        default:
                            throw new AssertionError("Invalid type " + arrayType);
                    }
                    break;
                default:
                    throw new AssertionError("Invalid type " + field.getType());
            }
        }
        return builder.build();
    }
}
