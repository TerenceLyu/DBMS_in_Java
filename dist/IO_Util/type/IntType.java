package IO_Util.type;
/*
 * #%L
 * nom.tam FITS library
 * %%
 * Copyright (C) 2004 - 2015 nom-tam-fits
 * %%
 * This is free and unencumbered software released into the public domain.
 * 
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 * 
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 * #L%
 */

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

class IntType extends PrimitiveTypeBase<IntBuffer> {

    private static final int BIT_PIX = 32;

    private static final int SIZE = 4;

    protected IntType() {
        super(SIZE, false, int.class, Integer.class, IntBuffer.class, 'I', BIT_PIX);
    }

    @Override
    public void appendBuffer(IntBuffer buffer, IntBuffer dataToAppend) {
        int[] temp = new int[Math.min(COPY_BLOCK_SIZE, dataToAppend.remaining())];
        while (dataToAppend.hasRemaining()) {
            int nrObBytes = Math.min(temp.length, dataToAppend.remaining());
            dataToAppend.get(temp, 0, nrObBytes);
            buffer.put(temp, 0, nrObBytes);
        }
    }

    @Override
    public IntBuffer asTypedBuffer(ByteBuffer buffer) {
        return buffer.asIntBuffer();
    }

    @Override
    public void getArray(IntBuffer buffer, Object array, int offset, int length) {
        buffer.get((int[]) array, offset, length);
    }

    @Override
    public Object newArray(int length) {
        return new int[length];
    }

    @Override
    public void putArray(IntBuffer buffer, Object array, int length) {
        buffer.put((int[]) array, 0, length);
    }

    @Override
    public IntBuffer sliceBuffer(IntBuffer buffer) {
        return buffer.slice();
    }

    @Override
    public IntBuffer wrap(Object array) {
        return IntBuffer.wrap((int[]) array);
    }
}
